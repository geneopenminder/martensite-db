// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use anyhow::{Result, bail, ensure};
use faiss::IdMap;
use faiss::index::IndexImpl;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::data::index::{
    CompositePkOAHashMapIdx, IdxMetaStruct, IdxType, PkIdx, RangeIdxStruct, RangeIndexKey,
    RangeMemIdx, get_pk_num_values, idx_type_to_str,
};
use crate::data::segment::{
    ColumnDescription, EntityHeader, FlatValStruct, KeyspaceStruct, MAGIC, SegmentMetaStruct,
    SegmentSummaryStruct, VarLenStruct, VectorStruct,
};
use crate::data::{TableStruct, U64OAHashMapIdx};
use crate::db::table::TableQueryHandler;
use crate::db::wal::Wal;
use crate::model::Column;
use crate::model::data_type::convert_prep_st_value;
use crate::protocol::{
    BQuery, Command, ErrorCode, Header, OperationId, Order, PK_FLAG, PROTOCOL_MAGIC,
    PreparedStatement, QueryCondition, QueryResult, SUCCESS_RESPONSE,
    create_prepared_columns_header, parse_http, parse_plain,
};
use crate::{as_bytes, e_header, new_column};

lazy_static! {
    pub static ref ENITY_ID: AtomicU32 = AtomicU32::new(0);
    pub static ref SEGMENT_ID: AtomicU32 = AtomicU32::new(0);
    static ref ST_ID: AtomicU32 = AtomicU32::new(0);
    static ref TX_ID: AtomicU64 = AtomicU64::new(0);
}

pub fn get_current_timestamp_millis() -> Result<u128, std::time::SystemTimeError> {
    let now = SystemTime::now();
    let duration_since_epoch = now.duration_since(UNIX_EPOCH)?;
    Ok(duration_since_epoch.as_millis())
}

pub static DB_STORAGE_VERSION: u32 = 0x1;
pub static DB_VERSION: u32 = 0x1;

pub static KS_META: &'static str = "keyspace_meta.db";
pub static TABLE_META: &'static str = "table_meta.db";

pub static SEGMENT_META: &'static str = "segment_meta.db";
pub static SEGMENT_SUMMARY: &'static str = "segment_summary.db";

pub static IDX_META: &'static str = "_idx_meta.db";

pub fn pk_idx_name(table_root: &String, segment_id: u32) -> String {
    format!("{}/{}.pk_idx", table_root, segment_id)
}

pub fn composite_pk_idx_name(table_root: &String, segment_id: u32) -> String {
    format!("{}/{}.composite_pk_idx", table_root, segment_id)
}

pub fn idx_meta_name(table_root: &String, idx_name: &String, idx_type: &IdxType) -> String {
    format!(
        "{}/{}_{}_idx_meta.db",
        table_root,
        idx_name,
        idx_type_to_str(idx_type)
    )
}

pub fn range_idx_name(
    table_root: &String,
    segment_id: u32,
    idx_name: &String,
    idx_type: &IdxType,
) -> String {
    format!(
        "{}/{}_{}.{}_idx",
        table_root,
        segment_id,
        idx_name,
        idx_type_to_str(idx_type)
    )
}

pub fn flat_name(table_root: &String, segment_id: u32, col_name: &String) -> String {
    format!("{}/{}_{}.flat", table_root, segment_id, col_name)
}

pub fn vec_name(table_root: &String, segment_id: u32, col_name: &String) -> String {
    format!("{}/{}_{}.vec", table_root, segment_id, col_name)
}

pub fn var_name(table_root: &String, segment_id: u32, col_name: &String) -> String {
    format!("{}/{}_{}.var", table_root, segment_id, col_name)
}

pub fn table_segment_meta_name(table_root: &String, segment_id: u32) -> String {
    format!("{}/{}_{}", table_root, segment_id, SEGMENT_META)
}

pub fn table_segment_summary_name(table_root: &String, segment_id: u32) -> String {
    format!("{}/{}_{}", table_root, segment_id, SEGMENT_SUMMARY)
}

pub struct TableSegment {
    pub id: u32,
    pub meta: SegmentMetaStruct,
    pub summary: SegmentSummaryStruct,
    pub pk_idx: Box<dyn PkIdx + Sync + Send>,
    pub vectors: HashMap<u8, VectorStruct>,
    pub flats: HashMap<u8, FlatValStruct>,
    pub vars: HashMap<u8, VarLenStruct>,
    pub range_idxs: Vec<RangeIdxStruct>,
}

pub struct FaissIndex {
    pub columns: Vec<u8>,
    pub idx: IdMap<IndexImpl>,
}

pub struct ClientConnection {
    pub conn_fd: i32,
    pub client_version: u8,
    pub prep_statements: HashMap<u32, PreparedStatement>,
}

impl Hash for ClientConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.conn_fd.hash(state);
    }
}

impl PartialEq for ClientConnection {
    fn eq(&self, other: &Self) -> bool {
        self.conn_fd == other.conn_fd
    }
}

impl PartialOrd for ClientConnection {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Option::Some(self.conn_fd.cmp(&other.conn_fd))
    }
}

impl Ord for ClientConnection {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.conn_fd.cmp(&other.conn_fd)
    }
}

impl Eq for ClientConnection {}

pub struct DbMainState {
    db_root: String,
    table_handlers: HashMap<u32, TableQueryHandler>,

    keyspaces: HashMap<u32, KeyspaceStruct>,
    keyspaces_by_name: HashMap<String, u32>,

    tables: HashMap<u32, TableStruct>,
    tables_by_name: HashMap<String, u32>,
    tables_to_ks: HashMap<u32, u32>,

    table_columns: HashMap<u32, HashMap<u8, ColumnDescription>>,
    table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>>,
    client_connections: HashMap<i32, ClientConnection>,
    table_columns_names: HashMap<u32, HashMap<u8, Vec<u8>>>,
    wal: Wal,
}

impl Drop for DbMainState {
    fn drop(&mut self) {
        println!("DROP");
        self.wal
            .close(TX_ID.load(std::sync::atomic::Ordering::Acquire));

        &self.table_handlers.iter_mut().for_each(|f| {
            f.1.flush_memtable(true);
        });
    }
}

impl DbMainState {
    pub fn init(db_root: String) -> Result<Self> {
        let ts_id = get_current_timestamp_millis().unwrap();
        let mut table_handlers: HashMap<u32, TableQueryHandler> = HashMap::new();

        let mut keyspaces: HashMap<u32, KeyspaceStruct> = HashMap::new();
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();

        let mut tables: HashMap<u32, TableStruct> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_to_ks: HashMap<u32, u32> = HashMap::new();
        //let mut table_segments: HashMap<u64, LinkedList<TableSegment>> = HashMap::new();

        let mut table_columns: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_names: HashMap<u32, HashMap<u8, Vec<u8>>> = HashMap::new();

        let mut last_tx_id: u64 = ts_id as u64;
        let mut last_segment_id: u32 = 1000;
        let mut last_entity_id: u32 = 100;

        let mut wal_files: Vec<String> = vec![];
        for entry in fs::read_dir(&db_root)? {
            let entry = entry?;
            if (entry.path().is_file()) {
                let wal_file = String::from(entry.path().to_str().unwrap());
                if (wal_file.contains("wal")) {
                    println!("wal file {}", wal_file);
                    wal_files.push(wal_file);
                } else {
                    println!("unknown file {}", wal_file);
                }
            } else if (entry.path().is_dir()) {
                let keyspace_root = String::from(entry.path().to_str().unwrap());
                let ks_meta_path = format!("{}/{}", keyspace_root, KS_META);
                println!("keyspace meta {}", ks_meta_path);
                if (!Path::new(&ks_meta_path).exists()) {
                    bail!("no keyspace meta file {}", ks_meta_path);
                }

                let ks: KeyspaceStruct = KeyspaceStruct::load(&keyspace_root, &ks_meta_path)?;
                println!("keyspace {:?}", &ks);
                let ks_id = ks.header.id;

                if ks_id > last_entity_id {
                    last_entity_id = ks_id;
                }

                keyspaces_by_name.insert(ks.name.clone(), ks.header.id);
                keyspaces.insert(ks_id, ks);

                for entry in fs::read_dir(&keyspace_root)? {
                    if (!entry.as_ref().unwrap().path().is_dir()) {
                        continue;
                    }

                    let table_root = String::from(entry.as_ref().unwrap().path().to_str().unwrap());
                    let table_meta_path = format!("{}/{}", table_root, TABLE_META);
                    println!("table meta {}", table_meta_path);

                    if (!Path::new(&table_meta_path).exists()) {
                        bail!("no keyspace meta file {}", table_meta_path);
                    }

                    let mut table = TableStruct::load(&table_root, &table_meta_path)?;
                    println!("table {:?}", &table);
                    let t_id: u32 = table.header.id;

                    if t_id > last_entity_id {
                        last_entity_id = t_id;
                    }

                    let mut columns: HashMap<u8, ColumnDescription> = HashMap::new();
                    let mut cols_vec = vec![];
                    let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
                    let mut col_names: HashMap<u8, Vec<u8>> = HashMap::new();

                    for c in &table.columns {
                        let c_name = String::from_utf8(c.name.clone())?;
                        println!("column {}", c_name);

                        columns.insert(c.col_id, c.clone());
                        cols_vec.push(c.clone());
                        columns_by_name.insert(c_name, c.clone());
                        col_names.insert(c.col_id, c.name.clone());
                    }

                    tables_by_name.insert(table.name.clone(), t_id);
                    tables_to_ks.insert(t_id, ks_id);
                    table_columns.insert(t_id, columns.clone());
                    table_columns_by_name.insert(t_id, columns_by_name);
                    table_columns_names.insert(t_id, col_names);

                    let table_entries: fs::ReadDir = fs::read_dir(&table_root)?;
                    let indexes: Vec<String> = table_entries
                        .filter(|f| {
                            f.as_ref()
                                .unwrap()
                                .path()
                                .to_str()
                                .unwrap()
                                .contains(IDX_META)
                        })
                        .map(|f| {
                            return String::from(f.as_ref().unwrap().path().to_str().unwrap());
                        })
                        .collect();

                    let mut range_idxs_meta: Vec<IdxMetaStruct> = vec![];

                    for idx in indexes {
                        println!("load range idx {}", idx);
                        let meta = IdxMetaStruct::load(&idx)?;
                        println!("idx columns {:?}", meta.columns);
                        range_idxs_meta.push(meta);
                    }

                    let table_entries = fs::read_dir(&table_root)?;

                    let mut segment_ids: Vec<String> = table_entries
                        .filter(|f| {
                            f.as_ref()
                                .unwrap()
                                .path()
                                .to_str()
                                .unwrap()
                                .contains(SEGMENT_META)
                        })
                        .map(|f| {
                            println!("file {:?}", f.as_ref().unwrap().path().file_name().unwrap());
                            let s_p = String::from(
                                f.as_ref()
                                    .unwrap()
                                    .path()
                                    .file_name()
                                    .unwrap()
                                    .to_str()
                                    .unwrap(),
                            );
                            let idx = s_p.find(SEGMENT_META).unwrap();
                            let sub = &s_p[0..idx - 1];
                            return sub.to_string().clone();
                        })
                        .collect();

                    //from last to first
                    segment_ids.sort_by(|s1, s2| s2.cmp(s1));
                    println!("segments {:?}", segment_ids);

                    let mut seg_list: VecDeque<Arc<TableSegment>> = VecDeque::new();

                    for s_id in &segment_ids {
                        let segment_id: u32 = s_id
                            .parse::<u32>()
                            .expect("segment id should be numeric u32");
                        if last_segment_id < segment_id {
                            last_segment_id = segment_id;
                        }

                        let summary = SegmentSummaryStruct::load(&table_segment_summary_name(
                            &table_root,
                            segment_id,
                        ))?;
                        if (summary.tx_id_max > last_tx_id) {
                            last_tx_id = summary.tx_id_max;
                        }
                        let meta = SegmentMetaStruct::load(
                            &table_segment_meta_name(&table_root, segment_id),
                            &summary.columns,
                        )?;

                        let pk_size = table.pk_size;

                        let segment_pk_idx: Box<dyn PkIdx + Sync + Send> = if pk_size == 1 {
                            Box::new(U64OAHashMapIdx::load(&pk_idx_name(
                                &table_root,
                                segment_id,
                            ))?)
                        } else {
                            Box::new(CompositePkOAHashMapIdx::load(&composite_pk_idx_name(
                                &table_root,
                                segment_id,
                            ))?)
                        };

                        println!("segment id {} ; rows count {}", s_id, &summary.rows_count);
                        let pk_nums_from =
                            get_pk_num_values(&summary.pk_range.pk_bytes_from, &columns);
                        let pk_nums_to = get_pk_num_values(&summary.pk_range.pk_bytes_to, &columns);
                        println!(
                            "segment id {} ; compisite pk from {:?} to {:?}",
                            s_id, pk_nums_from, pk_nums_to
                        );
                        println!(
                            "segment id {} ; tx_id from {} to {}",
                            s_id, &summary.tx_id_min, &summary.tx_id_max
                        );

                        let mut flat_files: HashMap<u8, FlatValStruct> = HashMap::new();
                        let mut vector_files: HashMap<u8, VectorStruct> = HashMap::new();
                        let mut varl_files: HashMap<u8, VarLenStruct> = HashMap::new();

                        for c in &summary.columns {
                            let c_name = String::from_utf8(c.name.clone())?;

                            if (c.data_type.vector) {
                                let vec = VectorStruct::load(&vec_name(
                                    &table_root,
                                    segment_id,
                                    &c_name,
                                ))?;
                                vector_files.insert(c.col_id, vec);
                            } else if (c.data_type.var_length && c.dimension == 0) {
                                let varl = VarLenStruct::load(&var_name(
                                    &table_root,
                                    segment_id,
                                    &c_name,
                                ))?;
                                varl_files.insert(c.col_id, varl);
                            } else {
                                let flat = FlatValStruct::load(&flat_name(
                                    &table_root,
                                    segment_id,
                                    &c_name,
                                ))?;
                                flat_files.insert(c.col_id, flat);
                            }
                        }

                        let mut range_idxs: Vec<RangeIdxStruct> = vec![];
                        //idxs
                        if (!range_idxs_meta.is_empty()) {
                            for idx in &range_idxs_meta {
                                let path = range_idx_name(
                                    &table_root,
                                    segment_id,
                                    &idx.idx_name,
                                    &IdxType::Range,
                                );
                                println!("load idx segment {}", path);
                                let idx_segment = RangeIdxStruct::load(
                                    &path,
                                    &idx.columns
                                        .iter()
                                        .map(|c| {
                                            columns
                                                .get(c)
                                                .expect("idx should contain valid column ids")
                                                .clone()
                                        })
                                        .collect(),
                                    idx.keys_bitmap,
                                )?;
                                range_idxs.push(idx_segment);
                            }
                        }

                        let table_segment = TableSegment {
                            id: s_id.parse::<u32>()?,
                            meta: meta,
                            summary: summary,
                            pk_idx: segment_pk_idx,
                            vectors: vector_files,
                            flats: flat_files,
                            vars: varl_files,
                            range_idxs: range_idxs,
                        };
                        seg_list.push_front(Arc::new(table_segment));
                    }

                    let arc_segments = Arc::new(RwLock::new(seg_list));

                    let mut range_mem_idxs: Vec<RangeMemIdx> = vec![];

                    for idx in &range_idxs_meta {
                        let cc = idx
                            .columns
                            .iter()
                            .map(|c| {
                                columns
                                    .get(c)
                                    .expect("idx should contain valid column ids")
                                    .clone()
                            })
                            .collect();
                        range_mem_idxs.push(RangeMemIdx {
                            idx_id: idx.header.id,
                            keys_bitmap: idx.keys_bitmap,
                            idx_name: idx.idx_name.clone(),
                            columns: cc,
                            keys: BTreeMap::<u64, RangeIndexKey>::new(),
                        });
                    }

                    let mut table_handler = TableQueryHandler::new(
                        table.header.id,
                        table.pk_size,
                        table_root.clone(),
                        &columns,
                        &cols_vec,
                        arc_segments,
                        range_mem_idxs,
                    );

                    //table_handler.compact();

                    tables.insert(table.header.id, table);
                    table_handlers.insert(t_id, table_handler);
                }
            }
        }

        /*
        //process WAL TODO
        for w in &wal_files {
            let header = WalSegment::read_records(w);
            if (header.end_tx_id > last_tx_id) {
                last_tx_id = header.end_tx_id;
            }
            println!("wal tx from {} to {} ; records count {}", header.start_tx_id, header.end_tx_id, header.records_count);

            println!("remove wal {}", w);
            fs::remove_file(w);
        }
        */

        TX_ID.store(last_tx_id + 1000, std::sync::atomic::Ordering::SeqCst);
        SEGMENT_ID.store(
            last_segment_id + 1 as u32,
            std::sync::atomic::Ordering::Release,
        );
        ENITY_ID.store(
            last_entity_id + 1 as u32,
            std::sync::atomic::Ordering::Release,
        );

        println!(
            "current tx_id: {}",
            TX_ID.load(std::sync::atomic::Ordering::Acquire)
        );

        let wal = Wal::new(&db_root, DB_STORAGE_VERSION);

        Ok(DbMainState {
            db_root: db_root,
            table_handlers: table_handlers,
            keyspaces: keyspaces,
            keyspaces_by_name: keyspaces_by_name,
            tables: tables,
            tables_by_name,
            tables_to_ks,
            table_columns,
            table_columns_by_name,
            client_connections: HashMap::new(),
            table_columns_names: table_columns_names,
            wal: wal,
        })
    }

    pub fn drop_connection(&mut self, client_fd: i32) {
        self.client_connections.remove(&client_fd);
    }

    pub fn process_query(
        &mut self,
        header: Header,
        payload: &[u8],
        out: &mut Vec<u8>,
        client_fd: i32,
    ) -> Result<QueryResult> {
        if (header.magic != PROTOCOL_MAGIC) {
            //REST reques, response should be plain
            let query_string = parse_http(&payload);
            match query_string {
                Ok(q) => {
                    let converted = parse_plain(
                        q,
                        false,
                        &self.keyspaces_by_name,
                        &self.tables_by_name,
                        &self.table_columns_by_name,
                        &self.table_columns,
                    );
                    match converted {
                        Ok(r) => {
                            return self.process_http_query(r);
                        }
                        Err(e) => {
                            return Ok(QueryResult::Plain {
                                response: format!("bad sql query: {}", e).as_bytes().to_vec(),
                            });
                        }
                    }
                }
                Err(e) => {
                    return Ok(QueryResult::Plain {
                        response: as_bytes!("bad query string"),
                    });
                }
            }
        }

        let cmd = Command::try_from(header.cmd)?;

        if header.cmd != Command::CreateConnection as u8
            && self.client_connections.get(&client_fd).is_none()
        {
            return Ok(QueryResult::Simple {
                err: ErrorCode::CONNECTION_NOT_EXIST,
            });
        }
        let mut connection = self.client_connections.get(&client_fd);

        match cmd {
            Command::PlainQuery => {
                let plain = String::from_utf8(payload.to_vec())?;
                let converted = parse_plain(
                    plain,
                    false,
                    &self.keyspaces_by_name,
                    &self.tables_by_name,
                    &self.table_columns_by_name,
                    &self.table_columns,
                )?;
                return self.process_internal_query(converted, out);
            }
            Command::CreatePrepared => {
                println!("create prep");
                let plain = String::from_utf8(payload.to_vec())?;
                let query = parse_plain(
                    plain,
                    true,
                    &self.keyspaces_by_name,
                    &self.tables_by_name,
                    &self.table_columns_by_name,
                    &self.table_columns,
                )?;

                let values_count = match &query {
                    BQuery::UpdateQuery {
                        entity,
                        input,
                        conditions,
                    } => input.iter().filter(|i| i.value.is_empty()).count(),
                    BQuery::SelectQuery {
                        entity,
                        select_columns,
                        fast_path,
                        conditions,
                    } => match conditions {
                        Some(some) => some.iter().filter(|c| c.values.is_empty()).count(),
                        None => 0,
                    },
                    BQuery::DeleteQuery { entity, conditions } => match conditions {
                        Some(some) => some.iter().filter(|c| c.values.is_empty()).count(),
                        None => 0,
                    },
                    _ => {
                        return Ok(QueryResult::Simple {
                            err: ErrorCode::SQL_UNSUPPORTED,
                        });
                    }
                };
                let prep_id = ST_ID.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                let (header, s) = match &query {
                    BQuery::SelectQuery {
                        entity,
                        select_columns,
                        fast_path,
                        conditions,
                    } => {
                        let columns = self
                            .table_columns
                            .get(&entity.table_id)
                            .expect("DB state incorrect for prepared statement");
                        let names = self
                            .table_columns_names
                            .get(&entity.table_id)
                            .expect("DB state incorrect for prepared statement");
                        let mut header: Vec<u8> = vec![0; 1024];
                        let size = create_prepared_columns_header(
                            &select_columns,
                            columns,
                            names,
                            &mut header,
                        );
                        (Option::Some(header[0..size].to_vec()), size)
                    }
                    _ => (Option::None, 0),
                };

                let mut data: Vec<u8> = vec![];
                data.extend(u32::to_le_bytes(prep_id));
                data.push(values_count as u8);

                self.insert_prep(
                    client_fd,
                    prep_id,
                    PreparedStatement {
                        columns_header: header,
                        header_size: s as u32,
                        query,
                    },
                );
                return Ok(QueryResult::Payload { data: data });
            }
            Command::ExecPrepared => {
                let st_id = u32::from_le_bytes(payload[0..4].try_into()?);
                let prepared = connection.unwrap().prep_statements.get(&st_id);
                match prepared {
                    Some(p) => {
                        let full_query = self.parse_prepared(payload, p)?;
                        return self.process_internal_query(full_query, out);
                    }
                    None => {
                        return Ok(QueryResult::Simple {
                            err: ErrorCode::PROTOCOL_BAD_PREPARED_ID,
                        });
                    }
                }
            }
            Command::CreateConnection => {
                println!("create connection msg");
                self.client_connections.insert(
                    client_fd,
                    ClientConnection {
                        conn_fd: client_fd,
                        client_version: header.version,
                        prep_statements: HashMap::new(),
                    },
                );
                return Ok(QueryResult::Simple {
                    err: ErrorCode::OK_OPCODE,
                });
            }
            _ => {
                return Ok(QueryResult::Simple {
                    err: ErrorCode::OK_OPCODE,
                });
            }
        }
    }

    pub fn insert_prep(&mut self, clint_fd: i32, st_id: u32, st: PreparedStatement) {
        let cc = self.client_connections.get_mut(&clint_fd).unwrap();
        cc.prep_statements.insert(st_id, st);
    }

    pub fn process_internal_query(
        &mut self,
        query: BQuery,
        out: &mut Vec<u8>,
    ) -> Result<QueryResult> {
        match query {
            BQuery::UpdateQuery {
                entity,
                input,
                conditions,
            } => {
                if (entity.operation == OperationId::INSERT) {
                    self.insert(entity.table_id, input);
                } else {
                    bail!(ErrorCode::SQL_UNSUPPORTED);
                }
            }
            BQuery::DeleteQuery { entity, conditions } => {
                if (entity.operation == OperationId::DELETE) {
                    self.delete_internal(entity.table_id, &conditions);
                } else {
                    bail!(ErrorCode::SQL_UNSUPPORTED);
                }
            }
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                let res = self.select_internal_streaming(
                    entity.table_id,
                    &conditions,
                    select_columns,
                    fast_path,
                    out,
                );

                match res {
                    Ok(s) => {
                        if (s == 0) {
                            bail!(ErrorCode::SQL_NOT_FOUND);
                        } else {
                            return Ok(QueryResult::Streaming { len: s });
                        }
                    }
                    Err(_) => {
                        bail!(ErrorCode::SYSTEM_UNKNOWN);
                    }
                }
            }
            BQuery::DdlQuery {
                entity,
                keyspace_name,
                table_name,
                columns_to_add,
                columns_to_drop,
                primary_key_size,
                with_range_idx,
            } => {
                println!("ks id {}", &entity.ks_id);
                if (entity.operation == OperationId::CREATE_KEYSPACE) {
                    self.create_keyspace(String::from_utf8(keyspace_name.unwrap()).unwrap())?
                } else if (entity.operation == OperationId::CREATE_TABLE) {
                    let name = String::from_utf8(table_name.unwrap()).unwrap();
                    let table_id = self.create_table(
                        entity.ks_id,
                        name.clone(),
                        columns_to_add.as_ref().unwrap().clone(),
                        primary_key_size,
                    )?;
                    if with_range_idx {
                        let idx_columns: Vec<u8> = columns_to_add
                            .unwrap()
                            .into_iter()
                            .filter(|c| c.flags & PK_FLAG > 0)
                            .map(|c| c.col_id)
                            .collect();
                        self.create_idx(
                            table_id,
                            name.clone(),
                            IdxType::Range,
                            idx_columns,
                            Option::None,
                        )?
                    }
                } else {
                    bail!(ErrorCode::SYSTEM_UNKNOWN);
                }
            }
            BQuery::CreateIndex {
                idx_name,
                entity,
                idx_type,
                columns,
                order,
            } => {
                self.create_idx(entity.table_id, idx_name, idx_type, columns, order)?;
            }

            BQuery::UNKNOWN {} => {
                bail!("unknown query")
            }
        }

        Ok(QueryResult::Simple {
            err: ErrorCode::OK_OPCODE,
        })
    }

    pub fn process_http_query(&mut self, query: BQuery) -> Result<QueryResult> {
        match query {
            BQuery::UpdateQuery {
                entity,
                input,
                conditions,
            } => {
                if (entity.operation == OperationId::INSERT) {
                    self.insert(entity.table_id, input);
                    return Ok(QueryResult::Plain {
                        response: SUCCESS_RESPONSE.as_bytes().to_vec(),
                    });
                } else {
                    return Ok(QueryResult::Plain {
                        response: format!("error code: {}", ErrorCode::SYSTEM_UNKNOWN as u8)
                            .as_bytes()
                            .to_vec(),
                    });
                }
            }
            BQuery::DeleteQuery { entity, conditions } => {
                if (entity.operation == OperationId::DELETE) {
                    self.delete_internal(entity.table_id, &conditions)?;
                    return Ok(QueryResult::Plain {
                        response: SUCCESS_RESPONSE.as_bytes().to_vec(),
                    });
                } else {
                    return Ok(QueryResult::Plain {
                        response: format!("error code: {}", ErrorCode::SYSTEM_UNKNOWN as u8)
                            .as_bytes()
                            .to_vec(),
                    });
                }
            }
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                let r = self.select_internal_http(
                    entity.table_id,
                    &conditions,
                    select_columns,
                    fast_path,
                );
                match r {
                    Ok(data) => {
                        return Ok((QueryResult::Plain { response: data }));
                    }
                    Err(_) => {
                        return Ok(QueryResult::Plain {
                            response: format!("error code: {}", ErrorCode::SYSTEM_UNKNOWN as u8)
                                .as_bytes()
                                .to_vec(),
                        });
                    }
                }
            }
            BQuery::DdlQuery {
                entity,
                keyspace_name,
                table_name,
                columns_to_add,
                columns_to_drop,
                primary_key_size,
                with_range_idx,
            } => {
                println!("ks id {}", &entity.ks_id);
                if (entity.operation == OperationId::CREATE_KEYSPACE) {
                    self.create_keyspace(String::from_utf8(keyspace_name.unwrap())?)?;
                } else if (entity.operation == OperationId::CREATE_TABLE) {
                    let table_id = self.create_table(
                        entity.ks_id,
                        String::from_utf8(table_name.as_ref().unwrap().clone())?,
                        columns_to_add.as_ref().unwrap().clone(),
                        primary_key_size,
                    );
                    if with_range_idx {
                        let idx_columns: Vec<u8> = columns_to_add
                            .unwrap()
                            .into_iter()
                            .filter(|c| c.flags & PK_FLAG > 0)
                            .map(|c| c.col_id)
                            .collect();
                        self.create_idx(
                            table_id.unwrap(),
                            String::from_utf8(table_name.unwrap()).unwrap(),
                            IdxType::Range,
                            idx_columns,
                            Option::None,
                        )?;
                    }
                } else {
                    return Ok(QueryResult::Plain {
                        response: format!("error code: {}", ErrorCode::SYSTEM_UNKNOWN as u8)
                            .as_bytes()
                            .to_vec(),
                    });
                }

                return Ok(QueryResult::Plain {
                    response: SUCCESS_RESPONSE.as_bytes().to_vec(),
                });
            }
            BQuery::CreateIndex {
                idx_name,
                entity,
                idx_type,
                columns,
                order,
            } => {
                return Ok(QueryResult::Plain {
                    response: format!("error code: {}", ErrorCode::SQL_UNSUPPORTED as u8)
                        .as_bytes()
                        .to_vec(),
                });
            }

            BQuery::UNKNOWN {} => {
                return Ok(QueryResult::Plain {
                    response: format!("error code: {}", ErrorCode::SYSTEM_UNKNOWN as u8)
                        .as_bytes()
                        .to_vec(),
                });
            }
        }
    }

    pub fn parse_prepared(&self, payload: &[u8], prepared: &PreparedStatement) -> Result<BQuery> {
        let st_id = u32::from_le_bytes(payload[0..4].try_into()?);
        let val_count = u32::from_le_bytes(payload[4..8].try_into()?);

        let payload_len = payload.len() as u64;
        //println!("data {:?}", data);

        let mut offset: u64 = 8;
        ensure!(offset < payload_len, "corrupted message");

        match &prepared.query {
            BQuery::UpdateQuery {
                entity,
                input,
                conditions,
            } => {
                if (val_count != input.len() as u32) {
                    bail!("not all values exist")
                }

                let mut new_columns: Vec<Column> = vec![];
                for (i, c) in input.iter().enumerate() {
                    if c.placeholder {
                        let (len, val) =
                            convert_prep_st_value(&payload, c.d_type, c.dimension, offset)?;
                        //println!("parse c {} {:?} len {}", c.col_id, val, len);

                        let p = input.get(i).unwrap();
                        let new_c = new_column!(p.col_id, p.d_type, p.dimension, p.flags, val);
                        new_columns.push(new_c);
                        offset += len as u64;
                    } else {
                        //println!("fixed value {:?}", c);
                    }
                }

                Ok(BQuery::UpdateQuery {
                    entity: entity.clone(),
                    input: new_columns,
                    conditions: Option::None,
                })
            }
            BQuery::SelectQuery {
                entity,
                select_columns,
                conditions,
                fast_path,
            } => {
                let mut full_conditions: Vec<QueryCondition> = vec![];

                for c in conditions.as_ref().unwrap() {
                    if c.placeholder {
                        let cond_column = self
                            .table_columns
                            .get(&entity.table_id)
                            .unwrap()
                            .get(&c.col_id)
                            .unwrap();
                        let (len, val) = convert_prep_st_value(
                            &payload,
                            cond_column.data_type.id,
                            cond_column.dimension,
                            offset,
                        )?;
                        offset += len as u64;
                        let cond = QueryCondition {
                            c_type: c.c_type.clone(),
                            col_id: c.col_id,
                            values: vec![val],
                            placeholder: true,
                        };
                        full_conditions.push(cond);
                    } else {
                        let cond = QueryCondition {
                            c_type: c.c_type.clone(),
                            col_id: c.col_id,
                            values: c.values.clone(),
                            placeholder: false,
                        };
                        full_conditions.push(cond);
                    }
                }

                Ok(BQuery::SelectQuery {
                    entity: entity.clone(),
                    select_columns: select_columns.clone(),
                    fast_path: *fast_path,
                    conditions: Option::Some(full_conditions),
                })
            }
            _ => bail!("bad query type"),
        }
    }

    //curl -X GET "http://127.0.0.1:8081/query?create%20schema%20ks1"
    pub fn create_keyspace(&mut self, name: String) -> Result<()> {
        println!("create keyspace {}", name);

        let id = ENITY_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let ts = get_current_timestamp_millis().unwrap() as u64;
        println!("create keyspace id {}", id);

        let ks_path = self.db_root.clone() + &name + "-" + &id.to_string().clone() + "/";
        let meta_path = format!("{}keyspace_meta.db", ks_path);

        fs::create_dir(&ks_path);

        let ks: KeyspaceStruct = KeyspaceStruct::new(
            &ks_path,
            &meta_path,
            &e_header!(id, MAGIC, DB_STORAGE_VERSION, ts),
            &name,
        )?;

        self.keyspaces.insert(id, ks);
        self.keyspaces_by_name.insert(name, id);

        Ok(())
    }

    //curl -X GET "http://127.0.0.1:8081/query?create%20table%20ks1.long_vectors%20(pk%20long,vec%20long_vector\[16\],uud%20json\[8\],%20%20primary%20key%20(pk))"
    //curl -X GET "http://127.0.0.1:8081/query?create%20table%20ks1.blobs%20(pk%20long%20primary%20key,bytes%20blob)"
    //curl -X GET "http://127.0.0.1:8081/query?create%20table%20ks1.time_series%20(pk%20long,ts%20long,uud%20json\[8\],%20%20primary%20key%20(pk,ts))"
    pub fn create_table(
        &mut self,
        ks_id: u32,
        name: String,
        columns: Vec<ColumnDescription>,
        primary_key_size: u8,
    ) -> Result<u32> {
        let id = ENITY_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let ts = get_current_timestamp_millis().unwrap() as u64;
        let table_path = self.keyspaces.get(&ks_id).unwrap().root.clone()
            + "/"
            + &name
            + "-"
            + &id.to_string().clone()
            + "/";
        let meta_path = format!("{}table_meta.db", table_path);

        fs::create_dir(&table_path)?;

        let table = TableStruct::new(
            &table_path,
            &meta_path,
            &e_header!(id, MAGIC, DB_STORAGE_VERSION, ts),
            &name,
            0,
            &columns.clone(),
            primary_key_size,
        )?;

        let arc_segments = Arc::new(RwLock::new(VecDeque::new()));
        let mut columns: HashMap<u8, ColumnDescription> = HashMap::new();
        let mut cols_vec = vec![];
        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        let mut col_names: HashMap<u8, Vec<u8>> = HashMap::new();

        for c in &table.columns {
            let c_name = String::from_utf8(c.name.clone())?;
            println!("column {}", c_name);

            columns.insert(c.col_id, c.clone());
            cols_vec.push(c.clone());
            columns_by_name.insert(c_name, c.clone());
            col_names.insert(c.col_id, c.name.clone());
        }

        let mut table_handler = TableQueryHandler::new(
            id,
            primary_key_size,
            table_path.clone(),
            &columns,
            &cols_vec,
            arc_segments,
            vec![],
        );

        self.tables_by_name.insert(name.clone(), id);
        self.tables_to_ks.insert(id, ks_id);
        self.table_columns.insert(id, columns);
        self.table_columns_by_name.insert(id, columns_by_name);
        self.table_columns_names.insert(id, col_names);
        self.tables.insert(id, table);
        self.table_handlers.insert(id, table_handler);

        Ok(id)
    }

    //curl -X GET "http://127.0.0.1:8081/query?insert%20into%20ks1.long_vectors%20(pk,vec)%20values%20(100,'23,34,56,78,1,2,3,4')"
    //curl -X GET "http://127.0.0.1:8081/query?insert%20into%20ks1.time_series%20(pk,ts,uud)%20values%20(100,200,'uuuiiiidddd')"
    pub fn insert(&mut self, tbl_id: u32, payload: Vec<Column>) {
        let tbl_h = self.table_handlers.get_mut(&tbl_id).unwrap();
        let tx_id = TX_ID.fetch_add(1, std::sync::atomic::Ordering::AcqRel); //TODO
        let ts = get_current_timestamp_millis().unwrap() as u64;

        //self.wal.write_record(*self.tables_to_ks.get(&tbl_id).unwrap(), tbl_id, &row);

        tbl_h.process_insert(tx_id, payload, ts);
    }

    //curl -X GET "http://127.0.0.1:8081/query?select%20*%20from%20ks1.long_vectors%20where%20pk=100"
    //curl -X GET "http://127.0.0.1:8081/query?select%20*%20from%20ks1.time_series%20where%20pk=100%20and%20ts=200"
    pub fn select_internal_streaming(
        &mut self,
        tbl_id: u32,
        conditions: &Option<Vec<QueryCondition>>,
        select_columns: Vec<u8>,
        fast_path: bool,
        out: &mut Vec<u8>,
    ) -> Result<usize> {
        let col_names = self.table_columns_names.get(&tbl_id).unwrap();
        let tbl_h = self.table_handlers.get_mut(&tbl_id).unwrap();

        tbl_h.process_select_streaming(conditions, col_names, select_columns, fast_path, out)
    }

    pub fn select_internal_http(
        &mut self,
        tbl_id: u32,
        conditions: &Option<Vec<QueryCondition>>,
        select_columns: Vec<u8>,
        fast_path: bool,
    ) -> Result<Vec<u8>> {
        let col_names = self.table_columns_names.get(&tbl_id).unwrap();
        let tbl_h = self.table_handlers.get_mut(&tbl_id).unwrap();

        //let (pk, parsed): (u64, Vec<u8>) = parse_value(column.data_type, value, column.dimension);

        tbl_h.process_select_http(conditions, col_names, select_columns, fast_path)
    }

    pub fn create_idx(
        &mut self,
        tbl_id: u32,
        idx_name: String,
        idx_type: IdxType,
        columns: Vec<u8>,
        order: Option<Order>,
    ) -> Result<()> {
        let mut tbl_h = self.table_handlers.get_mut(&tbl_id).unwrap();

        let table = self.tables.get(&tbl_id).expect("DB state invalid");

        let idx_id = ENITY_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let ts = get_current_timestamp_millis().unwrap() as u64;
        let meta_path = idx_meta_name(&table.root, &idx_name, &idx_type);

        let idx_meta = IdxMetaStruct::new(
            &meta_path,
            &idx_name,
            &e_header!(idx_id, MAGIC, DB_STORAGE_VERSION, ts),
            idx_type.clone(),
            &columns,
        )?;

        tbl_h.create_idx(idx_meta, idx_type, columns, order)
    }

    //curl -X GET "http://127.0.0.1:8081/query?delete%20from%20ks1.long_vectors%20whre%20pk=100)"
    pub fn delete_internal(
        &mut self,
        tbl_id: u32,
        conditions: &Option<Vec<QueryCondition>>,
    ) -> Result<QueryResult> {
        let col_names = self
            .table_columns_names
            .get(&tbl_id)
            .expect("DB state invalid");
        let tbl_h = self
            .table_handlers
            .get_mut(&tbl_id)
            .expect("DB state invalid");

        //let (pk, parsed): (u64, Vec<u8>) = parse_value(column.data_type, value, column.dimension);
        //let parsed = parse_num_pk(value, column.data_type); //TODO

        //println!("parsed pk {}", value);
        //println!("parsed pk num {}", parsed);
        //let col = Column { dimension: column.dimension, d_type: column.data_type, col_id: column.col_id, flags: column.flags,
        //    value: value.clone(), pk_num_val:  parsed};

        tbl_h.process_delete(conditions)
    }
}
