// // SPDX-License-Identifier: MIT
// // Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use faiss::{IdMap, MetricType, index_factory};
use lazy_static::lazy_static;
use skiplist::SkipMap;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use std::{fs, ptr, thread, u64};

use anyhow::{Result, bail};

use crate::data::U64OAHashMapIdx;
use crate::data::index::{
    CompositePkOAHashMapIdx, IdxMetaStruct, IdxType, PkIdx, PrimaryKey, RANGE_IDX_DATA_OFFSET,
    RangeIdxStruct, RangeIndexKey, RangeMemIdx, get_pk_values, pk_keys_bitmap, range_idx_insert,
    range_idx_select_conditions,
};
use crate::data::segment::{
    ColumnDescription, EntityHeader, FlatValStruct, MAGIC, PkRange, RowMeta, SegmentMetaStruct,
    SegmentSummaryStruct, VarLenStruct, VarLengthRowData, VectorStruct,
};
use crate::db::state::{
    DB_STORAGE_VERSION, SEGMENT_ID, TableSegment, composite_pk_idx_name, flat_name,
    get_current_timestamp_millis, pk_idx_name, range_idx_name, table_segment_meta_name,
    table_segment_summary_name, var_name, vec_name,
};
use crate::model::Column;
use crate::model::column::{Row, clone_column};
use crate::model::data_type::{
    BYTE, DataType, INT, LONG, SHORT, convert_to_num, map_data_type, value_to_string,
};
use crate::protocol::{
    ErrorCode, Order, QueryCondition, QueryConditionType, QueryResult, empty_response,
    prepare_rows_response, set_payload_size, write_rows,
};
use crate::{column_descr, e_header, new_column};

lazy_static! {
    static ref TX_ID: AtomicU64 = AtomicU64::new(0);
    static ref BYTES_INSERTED: AtomicU64 = AtomicU64::new(0);
    static ref MEMTABLE_LOCK: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
}

/// Query handler for particular table
/// Collect memtable & flush on treshold
/// for *_streaming it just write directly to spcket buffer without internal data representation

pub struct FlushedMemtable {
    pub segment_id: u32,
    pub memt: Arc<SkipMap<PrimaryKey, Arc<Row>>>,
}

pub struct TableQueryHandler {
    pub table_id: u32,
    pk_size: u8,
    table_root: String,
    segments: Arc<RwLock<VecDeque<Arc<TableSegment>>>>,
    segments_map: Arc<RwLock<HashMap<u32, Arc<TableSegment>>>>,
    keys_map: Arc<SkipMap<PrimaryKey, Arc<Row>>>,
    locked_segment: Arc<Mutex<Cell<Option<TableSegment>>>>,
    inflush_segments: Arc<RwLock<Vec<FlushedMemtable>>>,
    columns_by_id: HashMap<u8, ColumnDescription>,
    columns: Vec<ColumnDescription>,
    range_idxs: Arc<Vec<RangeMemIdx>>,
    range_idx_bitmaps: Arc<HashMap<u64, u32>>,
    pk_keys_bitmap: u64,
}

impl TableQueryHandler {
    pub fn new(
        table_id: u32,
        pk_size: u8,
        table_root: String,
        columns_by_id: &HashMap<u8, ColumnDescription>,
        columns: &Vec<ColumnDescription>,
        segments_list: Arc<RwLock<VecDeque<Arc<TableSegment>>>>,
        range_idxs: Vec<RangeMemIdx>,
    ) -> Self {
        let keys_bitmap: u64 = pk_keys_bitmap(columns);

        let mut idx_bitmaps: HashMap<u64, u32> = HashMap::new();
        for i in &range_idxs {
            idx_bitmaps.insert(i.keys_bitmap, i.idx_id);
        }

        let mut s_map: HashMap<u32, Arc<TableSegment>> = HashMap::new();

        let binding = Arc::clone(&segments_list);
        let segments = binding.read().unwrap();

        for s in segments.iter() {
            s_map.insert(s.id, Arc::clone(s));
        }

        TableQueryHandler {
            table_id: table_id,
            pk_size: pk_size,
            table_root: table_root,
            segments: segments_list,
            segments_map: Arc::new(RwLock::new(s_map)),
            keys_map: Arc::new(SkipMap::new()),
            locked_segment: Arc::new(Mutex::new(Cell::new(Option::None))),
            inflush_segments: Arc::new(RwLock::new(vec![])),
            columns: columns.clone(),
            columns_by_id: columns_by_id.clone(), //TODO
            range_idxs: Arc::new(range_idxs),
            pk_keys_bitmap: keys_bitmap,
            range_idx_bitmaps: Arc::new(idx_bitmaps),
        }
    }

    pub fn get_pk_fast_path(&self, conditions: &Vec<QueryCondition>) -> PrimaryKey {
        let c = conditions.get(0).unwrap();
        let column = self.columns_by_id.get(&c.col_id).unwrap();
        let value = c.values.get(0).unwrap();
        let pk = new_column!(
            column.col_id,
            column.data_type.id,
            column.dimension,
            column.flags,
            value.clone()
        );
        PrimaryKey {
            primary: pk,
            secondary: Option::None,
        }
    }

    pub fn convert_pk_internal(
        &self,
        conditions: &Vec<QueryCondition>,
        depth: usize,
    ) -> Vec<Vec<u64>> {
        let c = conditions.get(depth).unwrap();
        let mut nums: Vec<u64> = vec![];

        let d = self.columns_by_id.get(&c.col_id).unwrap();
        if matches!(c.c_type, QueryConditionType::IN) {
            for v in &c.values {
                let num = convert_to_num(v, d.data_type.id);
                nums.insert(0, num);
            }
        } else {
            let num = convert_to_num(c.values.get(0).unwrap(), d.data_type.id);
            nums.insert(0, num);
        }

        let mut final_nums: Vec<Vec<u64>> = vec![];

        if depth < conditions.len() - 1 {
            let res = self.convert_pk_internal(conditions, depth + 1);

            for n in nums {
                for v in &res {
                    let mut inter: Vec<u64> = vec![];
                    inter.push(n);
                    inter.extend(v);
                    final_nums.push(inter);
                }
            }
            return final_nums;
        } else {
            for n in nums {
                final_nums.push(vec![n]);
            }
        }
        final_nums
    }

    pub fn convert_pk(&self, conditions: &Vec<QueryCondition>) -> Result<Vec<PrimaryKey>> {
        println!("conds {:?}", conditions);
        let mut keys: Vec<PrimaryKey> = vec![];

        if self.pk_size == 1 {
            let primary_column = self.columns_by_id.get(&0).unwrap();
            let c = conditions.get(0).unwrap();

            if matches!(c.c_type, QueryConditionType::IN) {
                for v in &c.values {
                    keys.push(PrimaryKey {
                        primary: clone_column(primary_column, v.clone()),
                        secondary: Option::None,
                    });
                }
            } else {
                let value = conditions.get(0).as_ref().unwrap().values.get(0).unwrap();
                keys.push(PrimaryKey {
                    primary: clone_column(primary_column, value.clone()),
                    secondary: Option::None,
                });
            }
        } else {
            let pk_list = self.convert_pk_internal(conditions, 0);

            for k in pk_list {
                let mut columns: Vec<Column> = vec![];

                for (i, num) in k.iter().enumerate() {
                    let c = conditions.get(i).unwrap();
                    if matches!(c.c_type, QueryConditionType::IN) {
                        let d = self.columns_by_id.get(&c.col_id).unwrap();
                        let col = clone_column(d, u64::to_le_bytes(*num).to_vec());
                        columns.push(col);
                    } else {
                        let d = self.columns_by_id.get(&c.col_id).unwrap();
                        let col = clone_column(d, c.values.get(0).unwrap().clone());

                        columns.push(col);
                    }
                }
                keys.push(PrimaryKey {
                    primary: columns.remove(0),
                    secondary: Option::Some(columns),
                });
            }
        };

        Ok(keys)
    }

    fn select_by_range_memtable(
        &self,
        conditions: &Vec<QueryCondition>,
        idx_id: u32,
    ) -> Option<Vec<Arc<Row>>> {
        for r_i in self.range_idxs.iter() {
            if r_i.idx_id == idx_id {
                let mut columns: Vec<u64> = vec![];
                let mut conds = conditions.iter().map(|c| c.c_type.clone()).collect();
                for (i, c) in conditions.iter().enumerate() {
                    let col_id: u8 = c.col_id as u8;
                    let d = self.columns_by_id.get(&col_id).unwrap();
                    let value = c.values.get(0).unwrap().clone();
                    let num = convert_to_num(&value, d.data_type.id);
                    columns.push(num);
                }

                let mut response: Vec<Arc<Row>> = vec![];
                let ranges = self.convert_pk_internal(conditions, 0);
                for v in ranges {
                    let r = range_idx_select_conditions(&r_i.keys, &v, &conds, 0);
                    match r {
                        Some(rows) => response.extend(rows),
                        None => {}
                    }
                }

                if response.len() > 0 {
                    return Option::Some(response);
                }
            }
        }

        Option::None
    }

    fn select_by_range_segments(
        &self,
        conditions: &Vec<QueryCondition>,
        idx_id: u32,
    ) -> Option<Vec<(u32, RowMeta)>> {
        let binding = Arc::clone(&self.segments);
        let cloned = binding.read().unwrap();

        let mut col_types: Vec<&DataType> = vec![];
        for (i, c) in conditions.iter().enumerate() {
            let col_id: u8 = c.col_id as u8;
            let d = self.columns_by_id.get(&col_id).unwrap();
            col_types.push(map_data_type(d.data_type.id));
        }

        let mut row_metas: Vec<(u32, RowMeta)> = vec![];

        for s in cloned.iter() {
            for i in &s.range_idxs {
                if i.header.id == idx_id {
                    let idx_columns = &i.columns;
                    let ranges = self.convert_pk_internal(conditions, 0);
                    for columns in ranges {
                        let mut conds = conditions.iter().map(|c| c.c_type.clone()).collect();
                        let resp = i.find_range(
                            &columns,
                            &conds,
                            &col_types,
                            RANGE_IDX_DATA_OFFSET,
                            0,
                            idx_columns.len(),
                            0,
                            0,
                        );
                        println!("find range {:?}", resp);

                        for o in resp {
                            let row_meta: RowMeta = s.meta.get_row_meta(o);
                            println!("row meta {:?}", row_meta);
                            row_metas.push((s.id, row_meta));
                        }
                    }
                }
            }
        }
        if row_metas.len() > 0 {
            Option::Some(row_metas)
        } else {
            Option::None
        }
    }

    fn select_pk_segments(&self, pk: &PrimaryKey, fast_path: bool) -> Option<(u32, RowMeta)> {
        let pk_num: u64 = pk.primary.num_val;
        let miss_val = u64::MAX;

        let binding = Arc::clone(&self.segments);
        let cloned = binding.read().unwrap();

        for s in cloned.iter() {
            if fast_path {
                let pk_from = s.summary.pk_range.pk_num_from;
                let pk_to = s.summary.pk_range.pk_num_to;

                if (pk_num < pk_from || pk_num > pk_to) {
                    continue;
                }
            }

            let row_offset = s.pk_idx.get(&pk);

            //println!("row offset {}", row_offset);

            if (row_offset != miss_val) {
                let row_meta: RowMeta = s.meta.get_row_meta(row_offset);
                return Option::Some((s.id, row_meta));
            }
        }

        Option::None
    }

    pub fn process_select_streaming(
        &self,
        conditions: &Option<Vec<QueryCondition>>,
        col_names: &HashMap<u8, Vec<u8>>,
        select_columns: Vec<u8>,
        fast_path: bool,
        data: &mut Vec<u8>,
    ) -> Result<usize> {
        if (conditions.is_none()) {
            bail!("select without conditions not supported")
        }

        let mut query_col_bitmap: u64 = 0;

        for c in conditions.as_ref().unwrap() {
            query_col_bitmap += 0x1 << c.col_id;
        }

        let mut memtable_rows: Vec<Arc<Row>> = vec![];
        let mut segment_rows: Vec<(u32, RowMeta)> = vec![];

        if fast_path {
            let pk = self.get_pk_fast_path(&conditions.as_ref().unwrap());
            let mut row = self.keys_map.get(&pk);
            if row.is_some() {
                memtable_rows.push(Arc::clone(row.unwrap()));
            } else {
                let binding = self.inflush_segments.read().unwrap();
                let mut row: Option<Arc<Row>> = Option::None;
                for v in binding.iter() {
                    let r_mem = v.memt.get(&pk);
                    if r_mem.is_some() {
                        let r = Arc::clone(r_mem.unwrap());
                        row = Option::Some(r);
                        break;
                    }
                }

                if row.is_none() {
                    let result = self.select_pk_segments(&pk, fast_path);
                    match result {
                        Some((s, r)) => {
                            segment_rows.push((s, r));
                        }
                        None => (),
                    }
                } else {
                    memtable_rows.push(row.unwrap());
                }
            }
        } else {
            let pk_req = query_col_bitmap == self.pk_keys_bitmap
                && conditions.as_ref().unwrap().iter().all(|c| {
                    matches!(c.c_type, QueryConditionType::EQ)
                        || matches!(c.c_type, QueryConditionType::IN)
                });

            if pk_req {
                let pks = self.convert_pk(conditions.as_ref().unwrap())?;

                for pk in pks {
                    let mut row = self.keys_map.get(&pk);
                    if row.is_some() {
                        memtable_rows.push(Arc::clone(row.unwrap()));
                    } else {
                        let binding = self.inflush_segments.read().unwrap();
                        let mut row: Option<Arc<Row>> = Option::None;
                        for v in binding.iter() {
                            let r_mem = v.memt.get(&pk);
                            if r_mem.is_some() {
                                let r = Arc::clone(r_mem.unwrap());
                                row = Option::Some(r);
                                break;
                            }
                        }

                        if row.is_none() {
                            let result = self.select_pk_segments(&pk, fast_path);
                            match result {
                                Some((s, r)) => {
                                    segment_rows.push((s, r));
                                }
                                None => (),
                            }
                        } else {
                            memtable_rows.push(row.unwrap());
                        }
                    }
                }
            } else {
                println!("check idx");

                let range_query = self.range_idx_bitmaps.get(&query_col_bitmap);

                if range_query.is_none() {
                    bail!("not idx query")
                }

                let m = self
                    .select_by_range_memtable(conditions.as_ref().unwrap(), *range_query.unwrap());
                match m {
                    Some(rows) => {
                        memtable_rows = rows;
                    }
                    None => {}
                }
                let sr = self
                    .select_by_range_segments(conditions.as_ref().unwrap(), *range_query.unwrap());

                match sr {
                    Some(rows) => {
                        segment_rows = rows;
                    }
                    None => {}
                }
            }
        }

        let select_column_d: Vec<ColumnDescription> = select_columns
            .iter()
            .map(|c| self.columns_by_id.get(c).unwrap().clone())
            .collect();

        if memtable_rows.is_empty() && segment_rows.is_empty() {
            let size = empty_response(data);
            return Ok(size);
        }

        let mut offset = prepare_rows_response(col_names, &select_column_d, data);

        if !memtable_rows.is_empty() {
            offset = write_rows(&memtable_rows, &select_columns, data, offset);
        }

        let mut rows_count: u32 = memtable_rows.len() as u32;

        let binding = self.segments_map.read().unwrap();

        for (s_id, row_meta) in segment_rows {
            if (row_meta.not_null_cols == 0) {
                continue;
            }

            let s = binding.get(&s_id).unwrap();
            let idx = row_meta.idx;

            for c in &select_column_d {
                if !select_columns.contains(&c.col_id) {
                    continue;
                }

                data[offset] = c.col_id;
                offset += 1;

                let mut length: usize = 0;

                if (c.data_type.vector) {
                    let v = s.vectors.get(&c.col_id).unwrap();
                    length = v.copy_vector_bytes(data, idx as usize, offset);
                } else if (c.data_type.var_length) {
                    if (c.dimension > 0) {
                        let flat = s.flats.get(&c.col_id).unwrap();
                        let length_offset = offset;
                        offset += 4; //skip length 
                        length = flat.copy_bytes_val(data, idx as usize, offset);
                        data[length_offset..length_offset + 4]
                            .copy_from_slice(&u32::to_le_bytes(length as u32));
                    } else {
                        let mut found: bool = false;
                        for m in &row_meta.var_len_rows {
                            if (c.col_id == m.col_id) {
                                found = true;
                                let varl = s.vars.get(&c.col_id).unwrap();
                                let length_offset = offset;
                                offset += 4; //skip length 
                                length = varl.copy_bytes_val(data, m.file_offset as usize, offset);
                                data[length_offset..length_offset + 4]
                                    .copy_from_slice(&u32::to_le_bytes(length as u32));
                            }
                        }
                        if (!found) {
                            panic!("not found row meta for varl");
                        }
                    }
                } else {
                    let flat = s.flats.get(&c.col_id).unwrap();
                    length = flat.copy_bytes_val(data, idx as usize, offset);
                }
                offset += length;
            }

            //offset = write_row(&column_values, &self.columns_by_id, &mut data, offset);
            rows_count += 1;
        }

        set_payload_size(data, offset, rows_count);
        Ok(offset)
    }

    pub fn process_select_http(
        &self,
        conditions: &Option<Vec<QueryCondition>>,
        col_names: &HashMap<u8, Vec<u8>>,
        select_columns: Vec<u8>,
        fast_path: bool,
    ) -> Result<Vec<u8>> {
        println!("process select {:?}", conditions);

        if (conditions.is_none()) {
            bail!("no conditions in select")
        }

        let mut query_col_bitmap: u64 = 0;

        for c in conditions.as_ref().unwrap() {
            query_col_bitmap += 0x1 << c.col_id;
        }

        let mut memtable_rows: Vec<Arc<Row>> = vec![];
        let mut segment_rows: Vec<(u32, RowMeta)> = vec![];

        if fast_path {
            let pk = self.get_pk_fast_path(&conditions.as_ref().unwrap());
            let mut row = self.keys_map.get(&pk);
            if row.is_some() {
                if !row.as_ref().unwrap().deleted {
                    memtable_rows.push(Arc::clone(row.unwrap()));
                }
            } else {
                let binding = self.inflush_segments.read().unwrap();
                let mut row: Option<Arc<Row>> = Option::None;
                for v in binding.iter() {
                    let r_mem = v.memt.get(&pk);
                    if r_mem.is_some() {
                        if !r_mem.as_ref().unwrap().deleted {
                            let r = Arc::clone(r_mem.unwrap());
                            row = Option::Some(r);
                        }
                        break;
                    }
                }

                if row.is_none() {
                    let result = self.select_pk_segments(&pk, fast_path);
                    match result {
                        Some((s, r)) => {
                            segment_rows.push((s, r));
                        }
                        None => {}
                    }
                } else {
                    memtable_rows.push(row.unwrap());
                }
            }
        } else {
            let pk_req = query_col_bitmap == self.pk_keys_bitmap
                && conditions.as_ref().unwrap().iter().all(|c| {
                    matches!(c.c_type, QueryConditionType::EQ)
                        || matches!(c.c_type, QueryConditionType::IN)
                });

            if pk_req {
                let pks = self.convert_pk(conditions.as_ref().unwrap())?;

                for pk in pks {
                    let mut row = self.keys_map.get(&pk);
                    if row.is_some() {
                        if !row.as_ref().unwrap().deleted {
                            memtable_rows.push(Arc::clone(row.unwrap()));
                        }
                    } else {
                        let binding = self.inflush_segments.read().unwrap();
                        let mut row: Option<Arc<Row>> = Option::None;
                        for v in binding.iter() {
                            let r_mem = v.memt.get(&pk);
                            if r_mem.is_some() {
                                if !r_mem.as_ref().unwrap().deleted {
                                    let r = Arc::clone(r_mem.unwrap());
                                    row = Option::Some(r);
                                }
                                break;
                            }
                        }

                        if row.is_none() {
                            let result = self.select_pk_segments(&pk, fast_path);
                            match result {
                                Some((s, r)) => {
                                    if r.not_null_cols > 0 {
                                        segment_rows.push((s, r));
                                    }
                                }
                                None => {}
                            }
                        } else {
                            memtable_rows.push(row.unwrap());
                        }
                    }
                }
            } else {
                println!("check idx");

                let range_query = self.range_idx_bitmaps.get(&query_col_bitmap);

                if range_query.is_none() {
                    bail!("not index query")
                }

                let m = self
                    .select_by_range_memtable(conditions.as_ref().unwrap(), *range_query.unwrap());
                match m {
                    Some(rows) => {
                        memtable_rows = rows;
                    }
                    None => {}
                }
                let sr = self
                    .select_by_range_segments(conditions.as_ref().unwrap(), *range_query.unwrap());

                match sr {
                    Some(rows) => {
                        segment_rows = rows;
                    }
                    None => {}
                }
            }
        }

        let select_column_d: Vec<ColumnDescription> = select_columns
            .iter()
            .map(|c| self.columns_by_id.get(c).unwrap().clone())
            .collect();

        if memtable_rows.is_empty() && segment_rows.is_empty() {
            let rows_count = format!("Rows count: {}\n", 0);
            return Ok(rows_count.as_bytes().to_vec());
        }

        let mut rows_count: u32 = memtable_rows.len() as u32;

        let binding = self.segments_map.read().unwrap();

        let mut segment_payloads: Vec<Vec<(u8, Vec<u8>)>> = vec![];

        for (s_id, row_meta) in segment_rows {
            if (row_meta.not_null_cols == 0) {
                continue;
            }
            let s = binding.get(&s_id).unwrap();
            let idx = row_meta.idx;

            let mut column_values: Vec<(u8, Vec<u8>)> = vec![];

            for c in &select_column_d {
                if (c.data_type.vector) {
                    let v = s.vectors.get(&c.col_id).unwrap();
                    let vec_val: Vec<u8> = v.get_vector_bytes(idx as usize);
                    column_values.push((c.col_id, vec_val));
                } else if (c.data_type.var_length) {
                    if (c.dimension > 0) {
                        let flat = s.flats.get(&c.col_id).unwrap();
                        let val = flat.get_bytes_val(idx as u32);
                        column_values.push((c.col_id, val));
                    } else {
                        let mut found: bool = false;
                        for m in &row_meta.var_len_rows {
                            if (c.col_id == m.col_id) {
                                found = true;
                                let varl = s.vars.get(&c.col_id).unwrap();
                                let val = varl.read_bytes(m.file_offset);
                                column_values.push((c.col_id, val));
                            }
                        }
                        if (!found) {
                            panic!("not found row meta for varl");
                        }
                    }
                } else {
                    let flat = s.flats.get(&c.col_id).unwrap();
                    let val = flat.get_bytes_val(idx as u32);
                    column_values.push((c.col_id, val));
                }
            }
            segment_payloads.push(column_values);
            rows_count += 1;
        }

        let mut base_string = String::from("");

        let rows_count = format!("Rows count: {}\n", rows_count);
        base_string.push_str(&rows_count);

        for r in memtable_rows {
            base_string.push_str(&format!("row: {{"));
            for p in &r.payload {
                if select_columns.contains(&p.col_id) {
                    base_string.push_str("\"");
                    base_string.push_str(
                        String::from_utf8(col_names.get(&p.col_id).as_ref().unwrap().to_vec())
                            .unwrap()
                            .as_str(),
                    );
                    base_string.push_str("\":\"");

                    base_string.push_str(value_to_string(&p.value, p.d_type).as_str());
                    base_string.push_str("\",");
                }
            }
            base_string.pop();
            base_string.push_str(&format!("}}\n"));
        }

        for s in segment_payloads {
            base_string.push_str(&format!("row: {{"));
            for (c_id, data) in &s {
                if data.len() > 0 {
                    println!("val {:?}", data);
                    if select_columns.contains(c_id) {
                        base_string.push_str("\"");
                        base_string.push_str(
                            String::from_utf8(col_names.get(&c_id).as_ref().unwrap().to_vec())
                                .unwrap()
                                .as_str(),
                        );
                        base_string.push_str("\":\"");

                        base_string.push_str(
                            value_to_string(
                                data,
                                self.columns_by_id.get(c_id).unwrap().data_type.id,
                            )
                            .as_str(),
                        );
                        base_string.push_str("\",");
                    }
                }
            }
            base_string.pop();
            base_string.push_str(&format!("}}\n"));
        }
        Ok(base_string.as_bytes().to_vec())
    }

    pub fn process_delete(
        &mut self,
        conditions: &Option<Vec<QueryCondition>>,
    ) -> Result<QueryResult> {
        let tx_id = TX_ID.fetch_add(1, std::sync::atomic::Ordering::AcqRel); //TODO
        let ts = get_current_timestamp_millis().unwrap() as u64;

        let mut pks = self.convert_pk(conditions.as_ref().expect("DB state invalid"))?;

        let pk = pks.remove(0);
        println!("delete pk {:?}", pk);

        let row: Row = Row {
            pk: pk,
            timestamp: ts,
            tx_id,
            payload: vec![],
            deleted: true,
        };
        let rc = Arc::new(row);

        println!("delete");

        Arc::get_mut(&mut self.keys_map)
            .unwrap()
            .insert(rc.pk.clone(), rc);

        Ok(QueryResult::Simple {
            err: ErrorCode::OK_OPCODE,
        })
    }

    pub fn process_insert(
        &mut self,
        tx_id: u64,
        mut payload: Vec<Column>,
        ts: u64,
    ) -> Result<QueryResult> {
        let mut size = payload.iter().map(|c| c.value.len() as u64).sum();

        let memtable_size = BYTES_INSERTED.fetch_add(size, std::sync::atomic::Ordering::AcqRel);

        if (memtable_size > 0x1 << 28) {
            self.flush_memtable(false);
        }

        payload.sort_by(|a, b| a.col_id.cmp(&b.col_id));

        let pk_size = self.pk_size;

        let mut primary: Option<Column> = Option::None;
        let mut seconday_columns: Vec<Column> = vec![];

        for i in 0..pk_size {
            for c in &payload {
                if c.col_id == 0 {
                    primary = Option::Some(c.clone());
                } else if c.col_id == i {
                    seconday_columns.push(c.clone());
                }
            }
        }

        if (primary.is_none()) {
            bail!("no primary key")
        }

        let pk = PrimaryKey {
            primary: primary.unwrap(),
            secondary: if seconday_columns.is_empty() {
                Option::None
            } else {
                Option::Some(seconday_columns)
            },
        };
        //println!("insert pk {:?}", pk);

        let row: Row = Row {
            pk: pk,
            timestamp: ts,
            tx_id,
            payload: payload.clone(),
            deleted: false,
        };
        let rc = Arc::new(row);

        if (!self.range_idxs.is_empty()) {
            let idxs = Arc::get_mut(&mut self.range_idxs).unwrap();

            for i in idxs {
                let idx_column_ids: Vec<u8> = i.columns.iter().map(|c| c.col_id).collect();
                let mut idx_columns: Vec<&Column> = vec![];
                for c in &payload {
                    if idx_column_ids.contains(&c.col_id) {
                        idx_columns.push(c);
                    }
                }

                println!("insert idx {:?}", idx_columns);
                range_idx_insert(&mut i.keys, &idx_columns, &rc, 0);
            }
        }

        Arc::get_mut(&mut self.keys_map)
            .unwrap()
            .insert(rc.pk.clone(), rc);

        Ok(QueryResult::Simple {
            err: ErrorCode::OK_OPCODE,
        })
    }

    pub fn create_idx(
        &mut self,
        idx_meta: IdxMetaStruct,
        idx_type: IdxType,
        columns: Vec<u8>,
        order: Option<Order>,
    ) -> Result<()> {
        if matches!(idx_type, IdxType::VECTOR) {
            let mut index = index_factory(8, "Flat", MetricType::InnerProduct).unwrap();
            let mut index = IdMap::new(index).unwrap();

            let idx_id = SEGMENT_ID.fetch_add(10, std::sync::atomic::Ordering::AcqRel);

            let file_path = format!("{}/{}.vec_idx", self.table_root, idx_id);
            let created = get_current_timestamp_millis().unwrap() as u64;

            let idx_file = todo!(); //RangeIdxStruct::new(idx_id, file_path, idx_name, created, DB_VERSION, crate::model::data_type::IdxType::Range, todo!(), todo!());
        //self.vec_idx = Arc::new(Option::Some(VectorIdx { col: column.clone(), idx: index, idx_file }))
        } else if matches!(idx_type, IdxType::Range) {
            let idxs = Arc::get_mut(&mut self.range_idxs).unwrap();

            let mut idx_column_ids: Vec<ColumnDescription> = vec![];
            for c_id in columns {
                let c = self.columns_by_id.get(&c_id).unwrap();
                if c.data_type.var_length || c.data_type.vector {
                    bail!("unsupported");
                }

                idx_column_ids.push(c.clone());
            }

            let idx = RangeMemIdx {
                idx_id: idx_meta.header.id,
                keys_bitmap: idx_meta.keys_bitmap,
                idx_name: idx_meta.idx_name.clone(),
                columns: idx_column_ids,
                keys: BTreeMap::<u64, RangeIndexKey>::new(),
            };
            idxs.push(idx);
        } else {
            bail!("unsupported idx type")
        }

        Ok(())
    }

    pub fn process_select(&self, column: Column, condition: &QueryConditionType) -> Option<Row> {
        //println!("process select {:?}", column);

        let mut row: Option<Row> = Option::None; //self.keys_map.get(&column);

        let in_flush = self.inflush_segments.read().unwrap();

        if (row.is_none()) {
            for v in in_flush.iter() {

                //let r_mem = v.memt.get(&column);
                //if (r_mem.is_some()) {
                //    return Option::Some(r_mem.unwrap().clone());
                //}
            }
        }

        Option::None
    }

    //PANIC in case of errors
    pub fn flush_memtable(&mut self, wait: bool) {
        MEMTABLE_LOCK.lock().unwrap();

        println!("start flush");

        if (self.keys_map.len() == 0) {
            println!("memtable empty");
            return;
        }

        let memtable_size = BYTES_INSERTED.load(std::sync::atomic::Ordering::Acquire);

        /*
        //TODO only for multithread
        if (memtable_size > 0x1 << 24) {
        }*/

        BYTES_INSERTED.store(0, std::sync::atomic::Ordering::Release);

        //self.inflush_segments

        let segment_id = SEGMENT_ID.fetch_add(10, std::sync::atomic::Ordering::AcqRel);

        let infs = FlushedMemtable {
            segment_id,
            memt: Arc::clone(&self.keys_map),
        };

        self.inflush_segments.write().unwrap().push(infs);

        let to_flush = Arc::clone(&self.keys_map);

        println!("to flush keys size {}", to_flush.len());

        let table_columns = self.columns.clone();
        let table_columns_by_id = self.columns_by_id.clone();
        let table_root = self.table_root.clone();
        let current_segments = Arc::clone(&self.segments);
        let in_flush_cloned = Arc::clone(&self.inflush_segments);
        let idxs_to_flush = Arc::clone(&self.range_idxs);

        let pk_size: u8 = self.pk_size;
        //let vec_idx = &self.vec_idx;

        self.keys_map = Arc::new(SkipMap::new());

        let mut new_idxs: Vec<RangeMemIdx> = vec![];
        for i in idxs_to_flush.as_ref() {
            new_idxs.push(RangeMemIdx {
                idx_id: i.idx_id,
                keys_bitmap: i.keys_bitmap,
                idx_name: i.idx_name.clone(),
                columns: i.columns.clone(),
                keys: BTreeMap::<u64, RangeIndexKey>::new(),
            });
        }

        self.range_idxs = Arc::new(new_idxs);

        let t = thread::spawn(move || {
            let created = get_current_timestamp_millis().unwrap() as u64;

            let tx_id_min = to_flush.front().unwrap().1.tx_id;
            let tx_id_max = to_flush.back().unwrap().1.tx_id;

            let rows_count = to_flush.len() as u32;

            println!("flush segment {}", segment_id);

            let mut internal_columns: Vec<ColumnDescription> = vec![];

            let mut flat_files: HashMap<u8, FlatValStruct> = HashMap::new();
            let mut vector_files: HashMap<u8, VectorStruct> = HashMap::new();
            let mut varl_files: HashMap<u8, VarLenStruct> = HashMap::new();

            let mut meta = SegmentMetaStruct::new(
                &table_segment_meta_name(&table_root, segment_id),
                &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                rows_count,
                &table_columns,
            )
            .expect("can't create segment for memtable");

            for c in &table_columns {
                let col_name: String = String::from_utf8(c.name.clone()).unwrap();
                let dimension = c.dimension;
                println!("flush column {}", col_name);
                if (c.data_type.var_length) {
                    if (dimension == 0) {
                        let file_name = var_name(&table_root, segment_id, &col_name);
                        println!("var filename {}", file_name);
                        let mut varl = VarLenStruct::new(
                            &file_name,
                            &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                            *c.data_type,
                            c.col_id,
                            1024,
                        )
                        .expect("can't create segment for memtable");
                        varl_files.insert(c.col_id, varl);
                    } else {
                        let file_name = flat_name(&table_root, segment_id, &col_name);
                        println!("flat filename {}", file_name);
                        let mut flat = FlatValStruct::new(
                            &file_name,
                            &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                            *c.data_type,
                            dimension,
                            c.col_id,
                            rows_count,
                        )
                        .expect("can't create segment for memtable");
                        flat_files.insert(c.col_id, flat);
                    }
                } else if (c.data_type.vector) {
                    let file_name = vec_name(&table_root, segment_id, &col_name);
                    println!("filename {}", file_name);
                    let mut vector = VectorStruct::new(
                        &file_name,
                        &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                        *c.data_type,
                        dimension,
                        c.col_id,
                        rows_count,
                    )
                    .expect("can't create segment for memtable");
                    vector_files.insert(c.col_id, vector);
                } else {
                    let file_name = flat_name(&table_root, segment_id, &col_name);
                    println!("filename {}", file_name);
                    let mut flat = FlatValStruct::new(
                        &file_name,
                        &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                        *c.data_type,
                        dimension,
                        c.col_id,
                        rows_count,
                    )
                    .expect("can't create segment for memtable");
                    flat_files.insert(c.col_id, flat);
                }

                internal_columns.push(column_descr!(
                    c.col_id,
                    c.flags,
                    dimension,
                    c.data_type,
                    c.name.clone()
                ));
            }

            let mut idx_map: Box<dyn PkIdx + Sync + Send> = if pk_size == 1 {
                Box::new(
                    U64OAHashMapIdx::new(&pk_idx_name(&table_root, segment_id), rows_count)
                        .expect("cant create idx"),
                )
            } else {
                let mut pk_columns: Vec<ColumnDescription> = vec![];
                for i in 0..pk_size {
                    pk_columns.push(table_columns_by_id.get(&i).unwrap().clone());
                }
                Box::new(
                    CompositePkOAHashMapIdx::new(
                        &composite_pk_idx_name(&table_root, segment_id),
                        rows_count,
                        &pk_columns,
                    )
                    .expect("cant create idx"),
                )
            };

            let mut segment_range_idxs: Vec<RangeIdxStruct> = vec![];

            println!("to flush size {}", to_flush.len());
            for (s, (k, v)) in to_flush.iter().enumerate() {
                let idx: u32 = s as u32;
                let mut var_l_data: Vec<VarLengthRowData> = Vec::new();

                for c in &v.payload {
                    let data_type = map_data_type(c.d_type);

                    if (data_type.vector) {
                        let mut vector_s = vector_files.get_mut(&c.col_id).unwrap();
                        //println!("put vec idx {}", idx);
                        vector_s.put_vector_bytes(idx, &c.value);
                    } else if (data_type.var_length && c.dimension == 0) {
                        let mut varl = varl_files.get_mut(&c.col_id).unwrap();
                        let base_offset = varl.write_next_bytes(&c.value).unwrap();
                        var_l_data.push(VarLengthRowData {
                            file_offset: base_offset,
                            data_type: c.d_type,
                            col_id: c.col_id,
                        });
                    } else {
                        let mut flat = flat_files.get_mut(&c.col_id).unwrap();
                        //println!("put pk idx {} num {}", idx, c.pk_num_val);
                        flat.put_bytes_val(idx, &c.value);
                    }
                }

                //meta
                let offset = meta.insert_row_meta(
                    idx as u64,
                    v.tx_id,
                    v.timestamp,
                    v.deleted,
                    &v.payload,
                    &var_l_data,
                );
                idx_map.put(&k, offset);
            }

            meta.flush_and_remap();

            //let meta = SegmentMetaStruct::load(meta.path.clone());

            for i in idxs_to_flush.iter() {
                let idx_path =
                    range_idx_name(&table_root, segment_id, &i.idx_name, &IdxType::Range);
                let range_idx = RangeIdxStruct::new_with_data_mmap(
                    &idx_path,
                    &e_header!(i.idx_id, MAGIC, DB_STORAGE_VERSION, created),
                    segment_id,
                    &i.columns,
                    &i.keys,
                    &idx_map,
                )
                .expect("cant create idx");
                segment_range_idxs.push(range_idx);
            }

            idx_map.flush();

            flat_files
                .iter_mut()
                .for_each(|f| f.1.flush().expect("cant save data"));
            varl_files
                .iter_mut()
                .for_each(|f| f.1.flush_and_remap().expect("cant save data"));
            vector_files
                .iter_mut()
                .for_each(|f| f.1.flush().expect("cant save data"));

            println!("flush data finished");

            let pk_vals_from = get_pk_values(&to_flush.front().unwrap().0);
            let pk_vals_to = get_pk_values(&to_flush.back().unwrap().0);

            println!("pk from {:?} to {:?}", pk_vals_from.1, pk_vals_to.1);

            let range = PkRange {
                pk_num_from: *pk_vals_from.1.get(0).unwrap(),
                pk_num_to: *pk_vals_to.1.get(0).unwrap(),
                pk_composite_from: pk_vals_from.1,
                pk_composite_to: pk_vals_to.1,
                pk_bytes_from: pk_vals_from.0,
                pk_bytes_to: pk_vals_to.0,
                single_num: pk_size == 1,
            };

            let file_name = table_segment_summary_name(&table_root, segment_id);
            let mut summary = SegmentSummaryStruct::new(
                &file_name,
                &e_header!(segment_id, MAGIC, DB_STORAGE_VERSION, created),
                tx_id_min,
                tx_id_max,
                rows_count,
                &table_columns,
                range,
            )
            .expect("cant save data");

            let table_segment = TableSegment {
                id: segment_id,
                meta: meta,
                summary: summary,
                pk_idx: idx_map,
                vectors: vector_files,
                flats: flat_files,
                vars: varl_files,
                range_idxs: segment_range_idxs,
            };

            let mut binding = in_flush_cloned.write();
            let locked = binding.as_mut().unwrap();

            let mut new_segments = current_segments.write().unwrap();
            new_segments.push_back(Arc::new(table_segment));

            locked.retain(|s| s.segment_id != segment_id);

            drop(new_segments);
            drop(locked);
            println!("flush finished");
        });
        if (wait) {
            println!("wait flushing");
            t.join();
        }
    }

    pub fn convert_pk_num(&self, data_type: u8, data: &Vec<u8>) -> u64 {
        let mut pk_num: u64 = 0;
        if (data_type == LONG.id) {
            unsafe {
                let p = unsafe { data.as_ptr() as *const u64 };
                pk_num = u64::from_le(unsafe { ptr::read_unaligned(p) })
            }
        } else if (data_type == INT.id) {
            unsafe {
                let p = unsafe { data.as_ptr() as *const u32 };
                pk_num = u32::from_le(unsafe { ptr::read_unaligned(p) }) as u64
            }
        } else if (data_type == SHORT.id) {
            unsafe {
                let p = unsafe { data.as_ptr() as *const u16 };
                pk_num = u16::from_le(unsafe { ptr::read_unaligned(p) }) as u64
            }
        } else if (data_type == BYTE.id) {
            unsafe {
                let p = unsafe { data.as_ptr() as *const u8 };
                pk_num = u8::from_le(unsafe { ptr::read_unaligned(p) }) as u64
            }
        } else {
        };

        pk_num
    }

    fn read_row(&self, row_meta: &RowMeta, segment: &TableSegment) -> Row {
        let idx = row_meta.idx;
        //println!("row meta {:?}", row_meta);

        let mut payload: Vec<Column> = vec![];

        for c in &self.columns {
            if (c.data_type.vector) {
                let v = segment.vectors.get(&c.col_id).unwrap();
                let vec_val: Vec<u8> = v.get_vector_bytes(idx as usize);
                payload.push(clone_column(c, vec_val));
            } else if (c.data_type.var_length) {
                if (c.dimension > 0) {
                    let flat = segment.flats.get(&c.col_id).unwrap();
                    let val = flat.get_bytes_val(idx as u32);
                    payload.push(clone_column(c, val));
                } else {
                    let mut found = false;
                    for m in &row_meta.var_len_rows {
                        if (c.col_id == m.col_id) {
                            found = true;
                            let varl = segment.vars.get(&c.col_id).unwrap();
                            let val = varl.read_bytes(m.file_offset);
                            payload.push(clone_column(c, val));
                        }
                        if (!found) {
                            panic!("not found varl meta");
                        }
                    }
                }
            } else {
                let flat = segment.flats.get(&c.col_id).unwrap();
                let val = flat.get_bytes_val(idx as u32);
                let mut pk_num: u64 = 0;
                if (c.flags & 0x1 > 0) {
                    pk_num = self.convert_pk_num(c.data_type.id, &val);
                }
                payload.push(clone_column(c, val));
            }
        }

        Row {
            pk: PrimaryKey {
                primary: payload.get(0).unwrap().clone(),
                secondary: Option::None,
            },
            timestamp: row_meta.created,
            tx_id: row_meta.tx_id,
            payload,
            deleted: false,
        }
    }

    pub fn compact(&mut self) {
        let start = get_current_timestamp_millis().unwrap();
        println!("start compaction");

        let binding = Arc::clone(&self.segments);

        let mut deleted_keys: HashMap<u64, u64> = HashMap::new();

        let segments = binding.read().unwrap();
        let mut segments_ids: Vec<u32> = Vec::new();

        if (segments.len() < 5) {
            println!("skip compaction -- to low segments");
            return;
        }

        //from new to old
        for s in segments.iter() {
            segments_ids.push(s.id);
            /*
            let idx_map = &s.pk_idx;
            let keys = idx_map.get_keys();

            for k in keys {
                let meta_offset = idx_map.get(k);
                let meta = &s.meta.get_row_meta(meta_offset);
                if meta.not_null_cols == 0 {
                    deleted_keys.entry(k).or_insert(meta.tx_id);
                    println!("deleted {}", k);
                }
            }
            */
        }
        drop(segments);
        drop(binding);

        println!("compact segments");
        loop {
            let binding = Arc::clone(&self.segments);
            let segments = binding.read().unwrap();
            println!("segments before retain {}", segments.len());
            let mut rows: HashMap<PrimaryKey, Row> = HashMap::new();
            let mut segments_to_delete: Vec<u32> = vec![];

            for s in segments.iter() {
                if (!segments_ids.contains(&s.id)) {
                    continue;
                }

                segments_to_delete.push(s.id);

                let idx_map = &s.pk_idx;
                let keys = vec![]; //idx_map.get_keys();
                println!("compact idx keys count {} segment {}", keys.len(), s.id);

                for k in keys {
                    let meta_offset = 0; //idx_map.get(k);
                    let meta = &s.meta.get_row_meta(meta_offset);
                    let removed = deleted_keys.get(&k);

                    if meta.not_null_cols != 0 {
                        if (removed.is_some() && removed.unwrap() > &meta.tx_id) {
                            println!("remove key");
                        } else {
                            let row = self.read_row(meta, s);
                            rows.entry(row.pk.clone()).or_insert(row);
                        }
                    } else {
                        println!("removed key");
                    }
                }
                println!("rows {}", rows.len());

                if (rows.len() > 5000_000) {
                    break;
                }
            }
            drop(segments);
            drop(binding);

            if (rows.len() > 0) {
                println!("compact segments {:?}", segments_to_delete);
                println!("compact to new segment keys count {}", rows.len());

                let mut skip_rows: SkipMap<PrimaryKey, Arc<Row>> = SkipMap::new();
                for (k, v) in rows {
                    skip_rows.insert(k, Arc::new(v));
                }

                self.keys_map = Arc::new(skip_rows);
                self.flush_memtable(true);

                let binding = Arc::clone(&self.segments);
                let mut bwr = binding.write();
                let mut segments = bwr.as_mut().unwrap();
                println!("remove compacted segments {:?}", segments_to_delete);
                let cs: Vec<u32> = segments.iter().map(|s| s.id).collect();
                println!("all segments {:?}", cs);

                for s in segments.iter() {
                    if (segments_to_delete.contains(&s.id)) {
                        let idx = &s.pk_idx;
                        let idx_path = idx.path();
                        println!("remove idx file {}", idx_path);
                        fs::remove_file(&idx_path);

                        let meta = &s.meta;
                        println!("remove meta file {}", meta.path);
                        fs::remove_file(&meta.path);

                        let summary: &SegmentSummaryStruct = &s.summary;
                        println!("remove summary file {}", summary.path);
                        fs::remove_file(&summary.path);

                        for (k, v) in &s.vars {
                            println!("remove varl file {}", v.path);
                            fs::remove_file(&v.path);
                        }

                        for (k, v) in &s.vectors {
                            println!("remove vec file {}", v.path);
                            fs::remove_file(&v.path);
                        }

                        for (k, v) in &s.flats {
                            println!("remove flat file {}", v.path);
                            fs::remove_file(&v.path);
                        }
                    }
                }

                segments_ids.retain(|s| !segments_to_delete.contains(&s));
                segments.retain(|s| !segments_to_delete.contains(&s.id));

                println!("segments after retain {}", segments.len());
                drop(bwr.unwrap());
                drop(binding);
            } else {
                break;
            }
        }

        println!(
            "end compaction in ms - {}",
            get_current_timestamp_millis().unwrap() - start
        );
    }
}

// RUSTFLAGS="-Awarnings" cargo test test_create_table_struct -- --nocapture
#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        vec,
    };

    use crate::{
        column_descr,
        model::data_type::{BYTE_VECTOR, DataType, INT_VECTOR, JSON, LONG, LONG_VECTOR, SHORT},
    };

    use super::*;

    const PATH: &'static str = "/tmp/";
    const META_PATH: &'static str = "/tmp/ks_meta";

    #[test]
    fn test_insert() {
        let meta_name = format!("{}{}", META_PATH, get_current_timestamp_millis().unwrap());
        let mut columns_by_id: HashMap<u8, ColumnDescription> = HashMap::new();
        let mut columns_d: Vec<ColumnDescription> = Vec::new();

        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(
            0x0,
            0x1,
            0,
            &LONG,
            "pk_col".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x1,
            0x0,
            4,
            &BYTE_VECTOR,
            "vector".as_bytes().to_vec()
        ));

        let mut query_handler = TableQueryHandler::new(
            20,
            1,
            meta_name.clone(),
            &columns_by_id,
            &columns,
            Arc::new(RwLock::new(VecDeque::new())),
            vec![],
        );

        for i in 0..100 {
            let mut cols: Vec<Column> = Vec::new();
            let pk = new_column!(0x0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
            cols.push(pk.clone());
            let vec_val: Vec<u8> = [0; 4].to_vec();
            cols.push(new_column!(0x1, LONG_VECTOR.id, 0, 0x0, vec_val));

            query_handler.process_insert(1, cols, 100);
        }

        thread::sleep(Duration::from_secs(1));

        fs::remove_file(meta_name);
    }
}
