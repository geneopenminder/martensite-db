// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

use urlencoding::decode;

use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};

use anyhow::{Result, anyhow, bail};
use httparse::{Request, Status};

use crate::data::index::IdxType;
use crate::data::segment::{ColumnDescription, NAME_LENGTH_LIMIT};
use crate::model::Column;
use crate::model::column::Row;
use crate::model::data_type::{
    DATA_TYPES_MAP, INT, LONG, NUMERIC_ID_LIMIT, SHORT, UNKNOWN, parse_data_type, parse_str_value,
};
use crate::{column_descr, new_column, placeholder};

/// SQL parser & binary protocol
/// Supported types of queries -
/// CREATE KYSPACE
/// CREATE TABLE
/// INSERT
/// DELETE
/// CREATE INDEX (disabled)
/// AND, IN, >,<,>=,<=
///
/// examples
/// create table ks1.long_vectors (pk long, vec long_vector[8], uud json, primary key (pk))
/// create table ks1.long_vectors2 (pk long, pk2 int, uud json, primary key (pk, pk2)) with (range_idx=true) -- create uniq &  range idxs for PK
/// select * from ks1.long_vectors where pk = 500
/// select * from ks1.long_vectors where pk in (100,200)
/// delete from ks1.long_vectors where pk = 200
/// insert into ks1.long_vectors (pk,vec,uud) values (500,'23,34,56,78,1,2,3,4','dfghfghdfghdfghdfghfgdhdg')
///
/// create table ks1.time_series (pk long,ts long,uud json, primary key (pk,ts))  with (range_idx=true)
/// insert into ks1.time_series (pk,ts,uud) values (100,200,'uuuiiii200dddd')
/// select * from ks1.time_series where pk = 100 AND ts = 200
/// select * from ks1.time_series where pk = 100 AND ts in (200,300,400)
/// select * from ks1.time_series where pk = 100 AND ts > 300
/// select * from ks1.time_series where pk = 100 AND ts in (300,400)

pub const PROTOCOL_MAGIC: u8 = 0xAA;
pub const PROTOCOL_VERSION: u8 = 0x1;

pub static SUCCESS_RESPONSE: &'static str = "success";
pub static BAD_QUERY: &'static str = "bad query";
pub static DB_STATE_INCORRECT: &'static str = "db state incorrect";
static PK_TYPES: [u8; 3] = [LONG.id, INT.id, SHORT.id];

#[derive(Debug)]
pub struct CustomSqlDialect {}

impl Dialect for CustomSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '['
            || ch == ']'
            || ch == '?'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '$'
            || ch == '_'
            || ch == '?'
            || ch == '['
            || ch == ']'
    }
}

pub static PK_FLAG: u8 = 0x1;
pub static NOT_NULL_FLAG: u8 = 0x2;

#[derive(Debug, Clone, PartialEq)]
pub enum OperationId {
    CREATE_KEYSPACE = 1,
    DROP_KEYSPACE = 2,
    ALTER_KEYSPACE = 3,
    CREATE_TABLE = 4,
    DROP_TABLE = 5,
    ALTER_TABLE = 6,
    TRUNCATE_TABLE = 7,
    ALTER_TABLE_COLUMNS = 8,
    INSERT = 9,
    DELETE = 10,
    UPDATE = 11,
    CREATE_INDEX = 12,
    DROP_INDEX = 13,
    DESCRIBE = 14,

    SELECT = 20,
}

#[derive(Debug, Clone)]
pub struct QueryEntity {
    pub ks_id: u32,
    pub table_id: u32,
    pub operation: OperationId,
}

impl QueryEntity {
    pub fn new_keyspace() -> Self {
        Self {
            ks_id: 0,
            table_id: 0,
            operation: OperationId::CREATE_KEYSPACE,
        }
    }

    pub fn drop_keyspace(ks_id: u32) -> Self {
        Self {
            ks_id: ks_id,
            table_id: 0,
            operation: OperationId::DROP_KEYSPACE,
        }
    }

    pub fn new_table(ks_id: u32) -> Self {
        Self {
            ks_id: ks_id,
            table_id: 0,
            operation: OperationId::CREATE_TABLE,
        }
    }

    pub fn drop_table(ks_id: u32, table_id: u32) -> Self {
        Self {
            ks_id: ks_id,
            table_id: table_id,
            operation: OperationId::DROP_TABLE,
        }
    }
}

#[derive(Debug)]
pub enum Order {
    ASC,
    DESC,
}

#[derive(Debug)]
pub enum BQuery {
    UpdateQuery {
        entity: QueryEntity,
        input: Vec<Column>,
        conditions: Option<Vec<QueryCondition>>,
    },
    DeleteQuery {
        entity: QueryEntity,
        conditions: Option<Vec<QueryCondition>>,
    },
    SelectQuery {
        entity: QueryEntity,
        select_columns: Vec<u8>,
        fast_path: bool, //used for selection by single numeric primary key
        conditions: Option<Vec<QueryCondition>>,
    },
    CreateIndex {
        entity: QueryEntity,
        idx_name: String,
        idx_type: IdxType,
        columns: Vec<u8>,
        order: Option<Order>,
    },
    DdlQuery {
        entity: QueryEntity,
        keyspace_name: Option<Vec<u8>>,
        table_name: Option<Vec<u8>>,
        columns_to_add: Option<Vec<ColumnDescription>>,
        columns_to_drop: Option<Vec<u8>>,
        primary_key_size: u8,
        with_range_idx: bool,
    },
    UNKNOWN {},
}

#[derive(Debug)]
pub struct QueryCondition {
    pub c_type: QueryConditionType,
    pub col_id: u8,
    pub values: Vec<Vec<u8>>,
    pub placeholder: bool,
}

#[derive(Debug)]
pub struct QuerySimilarityCondition {
    pub c_type: QueryConditionType,
    pub col_id: u8,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum QueryConditionType {
    EQ,
    NEQ,
    GT,
    LT,
    GTE,
    LTE,
    IN,
    UNK,
    SimCosine,
}

#[derive(Debug)]
pub struct WhereConditionsNode {
    pub col_id: u8,
    pub operator: QueryConditionType,
    pub values: Vec<String>,
    pub placeholder: bool,
}

#[derive(Debug)]
pub struct EntityNode {
    pub ks_name: String,
    pub tbl_name: String,
    pub ks_id: u32,
    pub tbl_id: u32,
}

pub struct OrderByNode {
    pub col_id: u8,
    pub asc: bool,
}

#[derive(Debug)]
pub struct MoreConditionsNode {
    pub limit: u32,
    pub offset: u32,
    pub order_by: OrderBy,
    pub col_id: u8,
}

#[derive(Debug)]
pub enum OrderBy {
    ASC,
    DESC,
}

#[derive(Debug)]
pub enum Query {
    CreateTable {
        entity: EntityNode,
        columns: Vec<ColumnDescription>,
    },

    DropTable {
        entity: EntityNode,
    },

    TruncateTable {
        entity: EntityNode,
    },

    CreateKeyspace {
        name: String,
    },

    DropSchema {
        name: String,
    },

    Insert {
        entity: EntityNode,
        columns: Vec<u8>,
        values: Vec<String>,
    },

    Update {
        entity: EntityNode,
        columns: Vec<u8>,
        values: Vec<Vec<u8>>,
        conditions: Vec<WhereConditionsNode>,
    },

    Delete {
        entity: EntityNode,
        conditions: Vec<WhereConditionsNode>,
    },

    Select {
        entity: EntityNode,
        conditions: Vec<WhereConditionsNode>,
        more_cond: Option<MoreConditionsNode>,
    },

    CreateIndex {
        entity: EntityNode,
        columns: Vec<u8>,
    },

    Prepared {
        st_id: u32,
        payload: Vec<u8>,
    },

    Unknown {},
}

#[derive(Debug)]
pub struct PreparedStatement {
    pub columns_header: Option<Vec<u8>>,
    pub header_size: u32,
    pub query: BQuery,
}

#[macro_export]
macro_rules! as_bytes {
    ($msg:literal) => {
        $msg.as_bytes().to_vec()
    };
}

#[derive(Debug)]
pub enum QueryResult {
    Simple { err: ErrorCode },

    Error { err: ErrorCode, msg: String },

    Plain { response: Vec<u8> },

    Full { data: Vec<u8>, len: usize },

    Payload { data: Vec<u8> },

    Streaming { len: usize },
}

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum ErrorCode {
    OK_OPCODE = 0x00,
    SYSTEM_UNKNOWN = 0x01,
    SYSTEM_UNKNOWN_PROTOCOL_VERSION = 0x02,
    SYSTEM_OUT_OF_SPACE = 0x03,
    SYSTEM_OUT_OF_MEMORY = 0x04,
    SYSTEM_DISK_ERROR = 0x05,
    SYSTEM_CORRUPTED_FILE = 0x06,

    CONNECTION_CLOSED = 0x10,
    CONNECTION_NOT_EXIST = 0x11,

    PROTOCOL_BAD_MESSAGE_FORMAT = 0x20,
    PROTOCOL_BAD_MESSAGE_LENGTH = 0x21,
    PROTOCOL_BAD_OPCODE = 0x22,
    PROTOCOL_BAD_PREPARED_ID = 0x23,
    PROTOCOL_BAD_PREPARED_VALUE = 0x24,

    SQL_UNKNOWN_ENTITY = 0x30,
    SQL_WRONG_VALUE_FORMAT = 0x31,
    SQL_SYNTAX_ERROR = 0x32,
    SQL_DDL_ERROR = 0x33,
    SQL_UNKNOWN_DATA_TYPE = 0x34,
    SQL_NOT_FOUND = 0x35,
    SQL_UNSUPPORTED = 0x36,
}

impl Error for ErrorCode {}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::OK_OPCODE => {
                write!(f, "OK")
            }
            other_value => {
                write!(f, "Error: {}", other_value)
            }
        }
    }
}

#[derive(Debug)]
pub struct SqlErrorCode {
    pub msg: String,
}

impl SqlErrorCode {
    pub fn new(msg: &str) -> Self {
        SqlErrorCode {
            msg: msg.to_string(),
        }
    }
}

impl Error for SqlErrorCode {}

impl fmt::Display for SqlErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum Command {
    PlainQuery = 0x01,
    CreatePrepared = 0x02,
    ExecPrepared = 0x03,
    DeletePrepared = 0x04,
    BATCH = 0x05,
    RESULT = 0x06,
    CopyInit = 0x07,
    CopyData = 0x08,
    CopyDone = 0x09,
    ERROR = 0x0A,
    GetStatus = 0x0B,

    CreateConnection = 0x0C,
    CloseConnection = 0x0D,

    UNKNOWN = 0xFF,
}

impl TryFrom<u8> for Command {
    type Error = ErrorCode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x1 => Ok(Command::PlainQuery),
            0x2 => Ok(Command::CreatePrepared),
            0x3 => Ok(Command::ExecPrepared),
            0x4 => Ok(Command::DeletePrepared),
            0x0C => Ok(Command::CreateConnection),
            0x0D => Ok(Command::CloseConnection),
            0x0B => Ok(Command::GetStatus),
            _ => Err(ErrorCode::PROTOCOL_BAD_OPCODE),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub magic: u8,
    pub version: u8,
    pub cmd: u8,
    pub length: u32,
}

pub struct Payload<'a> {
    pub header: Header,
    pub data: &'a [u8],
}

pub static HEADER_SIZE: u32 = 8;

#[macro_export]
macro_rules! sql_err {
    ($msg:expr) => {
        Err(SqlErrorCode::new($msg).into())
    };
}

pub fn parse_plain(
    query: String,
    prepared: bool,
    keyspaces_by_name: &HashMap<String, u32>,
    tables_by_name: &HashMap<String, u32>,
    table_columns_by_name: &HashMap<u32, HashMap<String, ColumnDescription>>,
    table_columns_by_id: &HashMap<u32, HashMap<u8, ColumnDescription>>,
) -> Result<BQuery> {
    let dialect = CustomSqlDialect {};

    let decoded_string = decode(&query).map_err(|e| anyhow!("invalid utf-8 query string"))?;

    println!("decoded query string {}", decoded_string);
    let ast = Parser::parse_sql(&dialect, &decoded_string.replace("keyspace", "schema"))?; //TODO sql dialect not support keyspace keyword
    let ast_st = ast.into_iter().next();

    if ast_st.is_none() {
        return sql_err!(BAD_QUERY);
    }

    let st = ast_st.unwrap();
    println!("statement {:?}", st);

    let parsed_query: BQuery = match st {
        Statement::Delete {
            table_name,
            selection,
        } => {
            println!("delete {:?}", selection);
            let (ks_name, tbl_name, ks_id, tbl_id) = parse_table_ks_name(
                &table_name.to_string().to_lowercase(),
                keyspaces_by_name,
                tables_by_name,
            )?;
            let tbl_columns_by_name = table_columns_by_name
                .get(&tbl_id)
                .expect(DB_STATE_INCORRECT);
            let tbl_columns = table_columns_by_id.get(&tbl_id).expect(DB_STATE_INCORRECT);

            let mut conditions: Vec<WhereConditionsNode> = Vec::new();
            if let Some(selection) = selection {
                conditions.extend(parse_selection(&selection, tbl_columns_by_name)?);
            }
            let c = map_conditions(&conditions, tbl_columns, prepared)?;

            BQuery::DeleteQuery {
                entity: QueryEntity {
                    ks_id: 0,
                    table_id: tbl_id,
                    operation: OperationId::DELETE,
                },
                conditions: Option::Some(c),
            }
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => BQuery::DdlQuery {
            entity: QueryEntity::new_keyspace(),
            keyspace_name: Option::Some(schema_name.0.get(0).unwrap().value.as_bytes().to_vec()),
            table_name: Option::None,
            columns_to_add: Option::None,
            columns_to_drop: Option::None,
            primary_key_size: 0,
            with_range_idx: false,
        },
        Statement::Insert {
            or,
            table_name,
            columns,
            overwrite,
            source,
            partitioned,
            after_columns,
            table,
        } => {
            println!("ins {}", source.body);

            let mut cols: Vec<String> = Vec::new();
            let mut col_names: HashSet<String> = HashSet::new(); //uniq columns
            for c in columns {
                let c_name = c.value;
                cols.push(c_name.clone());

                if !col_names.insert(c_name.clone()) {
                    return sql_err!("column name dup");
                }
            }
            println!(" insert columns {:?}", cols);
            let mut c_values: Vec<String> = Vec::new();

            match source.body {
                SetExpr::Select(select) => return sql_err!(BAD_QUERY),
                SetExpr::Query(query) => {}
                SetExpr::SetOperation {
                    op,
                    all,
                    left,
                    right,
                } => {}
                SetExpr::Values(values) => {
                    if values.0.is_empty() || values.0.get(0).is_none() {
                        return sql_err!("bad query format");
                    }
                    for v in values.0.get(0).unwrap() {
                        c_values.push(v.to_string().replace("'", ""));
                    }
                }
                SetExpr::Insert(statement) => {}
            }

            if c_values.len() != col_names.len() {
                return sql_err!("wrong columns count");
            }

            let (ks_name, tbl_name, ks_id, tbl_id) = parse_table_ks_name(
                &table_name.to_string().to_lowercase(),
                keyspaces_by_name,
                tables_by_name,
            )?;
            let tbl_columns = table_columns_by_name
                .get(&tbl_id)
                .expect(DB_STATE_INCORRECT);

            let mut payload: Vec<Column> = vec![];

            for (i, c) in cols.iter().enumerate() {
                let dd = tbl_columns.get(c);
                if dd.is_none() {
                    return sql_err!("wrong column name");
                }

                let d = dd.unwrap();

                let str_val = c_values.get(i).unwrap();
                let is_placeholder: bool = str_val.len() == 1 && str_val.contains("?");

                if (is_placeholder && !prepared) {
                    return sql_err!("placeholder for not prepared statement");
                }

                if (is_placeholder) {
                    payload.push(placeholder!(d.col_id, d.data_type.id, d.dimension, d.flags));
                } else {
                    let (pk, parsed): (u64, Vec<u8>) =
                        parse_str_value(d.data_type.id, str_val, d.dimension)?;
                    let col = new_column!(d.col_id, d.data_type.id, d.dimension, d.flags, parsed);
                    payload.push(col);
                }
            }

            BQuery::UpdateQuery {
                entity: QueryEntity {
                    ks_id: 0,
                    table_id: tbl_id,
                    operation: OperationId::INSERT,
                },
                input: payload,
                conditions: Option::None,
            }
        }
        Statement::CreateTable {
            or_replace: _,
            temporary: _,
            external,
            if_not_exists,
            name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
        } => {
            println!("columns {:?}", columns);
            println!("tbl {}", name);

            let names = name.to_string().to_lowercase();
            let ks_table: Vec<&str> = names.split('.').collect();

            if ks_table.len() != 2 {
                return sql_err!("query should contain keyspace & table names");
            }

            let ks_name = String::from(*ks_table.get(0).unwrap());
            let table_name = String::from(*ks_table.get(1).unwrap());

            let ks_id: Option<&u32> = keyspaces_by_name.get(&ks_name);
            if ks_id.is_none() {
                return sql_err!(&format!("keyspace {} not exist", table_name));
            }

            let mut primary_keys_map: HashMap<String, u8> = HashMap::new();
            let mut primary_keys_vec: Vec<String> = vec![];

            if (constraints.len() > 1) {
                return sql_err!("unsupported num of constraints");
            } else if (constraints.len() == 1) {
                match constraints.get(0).unwrap() {
                    sqlparser::ast::TableConstraint::Unique {
                        name,
                        columns,
                        is_primary,
                    } => {
                        if (!*is_primary) {
                            return sql_err!("no primary key");
                        } else {
                            primary_keys_vec = columns.iter().map(|i| i.value.clone()).collect();

                            if (primary_keys_vec.len() > 3) {
                                return sql_err!("pk columns number limited to 3");
                            }

                            let mut idx: u8 = 0;
                            for c in &primary_keys_vec {
                                primary_keys_map.insert(c.clone().to_lowercase(), idx);
                                idx += 1;
                            }
                        }
                    }
                    _ => (),
                }
            } else {
                return sql_err!("no primary key definition");
            }

            let mut start_not_primary_col_id: u8 = primary_keys_map.len() as u8;
            let mut columns_vec: Vec<ColumnDescription> = Vec::new();

            let mut col_names: HashSet<String> = HashSet::new();

            for d in columns {
                let c_name = d.name.to_string().to_lowercase();
                if (c_name.len() > NAME_LENGTH_LIMIT) {
                    return sql_err!(&format!(
                        "column name length limit is {}",
                        NAME_LENGTH_LIMIT
                    ));
                }

                if !col_names.insert(c_name.clone()) {
                    return sql_err!("column name dup");
                }

                let pk = primary_keys_map.get(&c_name);
                let mut col_id: u8 = start_not_primary_col_id;
                let mut flags: u8 = 0;
                match pk {
                    Some(pk_id) => {
                        col_id = *pk_id;
                        flags |= PK_FLAG;
                    }
                    None => {
                        col_id = start_not_primary_col_id;
                        start_not_primary_col_id += 1;
                    }
                }

                let mut d_type_str = d.data_type.to_string().to_lowercase();

                let mut dimension: u16 = 0;
                if d_type_str.contains('[') {
                    let start_pos = d_type_str.find("[").unwrap_or(0) + 1;
                    let end_bytes = d_type_str.find("]"); //.unwrap_or(d_type_str.len());
                    match end_bytes {
                        Some(end_pos) => {
                            if end_pos <= start_pos {
                                return sql_err!("bad dimension definition");
                            }
                            dimension = d_type_str[start_pos..end_pos].parse().unwrap();
                            d_type_str = d_type_str[0..start_pos - 1].to_string();
                        }
                        None => return sql_err!("bad dimension definition"),
                    }
                }

                let data_type = parse_data_type(&d_type_str);
                if pk.is_some() && !PK_TYPES.contains(&data_type.id) {
                    return sql_err!("pk should be numeric type from short, int,long");
                }

                if data_type.id == UNKNOWN.id {
                    return sql_err!("bad data type");
                }
                columns_vec.push(column_descr!(
                    col_id,
                    flags,
                    dimension,
                    data_type,
                    c_name.as_bytes().to_vec()
                ));
            }

            println!("columns {:?}", columns_vec);

            //all pk columns exists
            for (c, i) in &primary_keys_map {
                if !col_names.contains(c) {
                    return sql_err!("pk contains unknown column name");
                }
            }

            let mut with_range_idx = false;
            for w_o in with_options {
                if w_o
                    .name
                    .to_string()
                    .to_lowercase()
                    .eq(&"range_idx".to_string())
                {
                    with_range_idx = true;
                }
            }

            BQuery::DdlQuery {
                entity: QueryEntity::new_table(*ks_id.unwrap()),
                keyspace_name: Option::None,
                table_name: Option::Some(table_name.as_bytes().to_vec()),
                columns_to_add: Option::Some(columns_vec),
                columns_to_drop: Option::None,
                primary_key_size: primary_keys_map.len() as u8,
                with_range_idx,
            }
        }
        Statement::Query(query) => {
            let mut entity: Option<EntityNode> = Option::None;
            let mut conditions: Vec<WhereConditionsNode> = Vec::new();

            let mut s_columns: Vec<String> = Vec::new();
            match query.body {
                SetExpr::Select(select) => {
                    // Match on the FROM clause
                    if let Some(table_with_joins) = select.from.first() {
                        match &table_with_joins.relation {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                let (ks_name, tbl_name, ks_id, tbl_id) = parse_table_ks_name(
                                    &name.to_string().to_lowercase(),
                                    keyspaces_by_name,
                                    tables_by_name,
                                )?;
                                entity = Option::Some(EntityNode {
                                    ks_name,
                                    tbl_name,
                                    ks_id,
                                    tbl_id,
                                });
                                println!("From table: {:?}", entity.as_ref().unwrap());
                                let column_descriptions = table_columns_by_name
                                    .get(&tbl_id)
                                    .expect(DB_STATE_INCORRECT);
                                conditions.extend(parse_selection(
                                    &select.selection.as_ref().unwrap(),
                                    &column_descriptions,
                                )?);
                            }
                            _ => return sql_err!("bad query selection format"),
                        }
                    }

                    if entity.is_none() {
                        return sql_err!("bad query selection format");
                    }

                    let col_by_name = table_columns_by_name
                        .get(&entity.as_ref().unwrap().tbl_id)
                        .expect(DB_STATE_INCORRECT);
                    for c in select.projection {
                        let col_str = c.to_string().to_lowercase();
                        if (col_str.len() > 1 && !col_str.contains("*")) {
                            if !col_by_name.contains_key(&col_str) {
                                return sql_err!("unknown column name");
                            }
                            s_columns.push(col_str);
                        }
                    }
                    println!("select columns {:?}", s_columns);
                }
                _ => return sql_err!("bad query selection format"),
            }

            let tbl_id = entity.unwrap().tbl_id;

            let table_columns = table_columns_by_id.get(&tbl_id).expect(DB_STATE_INCORRECT);
            if conditions.len() == 0 {
                return sql_err!("no conditions in query");
            }

            let mut conditions = map_conditions(&conditions, table_columns, prepared)?;

            let pk_size = table_columns
                .values()
                .filter(|c| c.flags & PK_FLAG > 0)
                .count();
            let pk_eq_query = conditions
                .iter()
                .filter(|c| {
                    matches!(c.c_type, QueryConditionType::EQ)
                        || !matches!(c.c_type, QueryConditionType::IN)
                })
                .count()
                == pk_size;

            let fast_path: bool = if conditions.len() > 1 || pk_size > 1 {
                false
            } else {
                let c = conditions.get(0).unwrap();
                matches!(c.c_type, QueryConditionType::EQ) && c.col_id == 0
            };

            let mut idx: u8 = 0;
            for cond in &conditions {
                if idx != cond.col_id {
                    return sql_err!(
                        "select conditions sequence should always be in order as primary key & idx key"
                    );
                }
                idx += 1;
            }

            if pk_eq_query && pk_size > idx as usize {
                return sql_err!("not all column for pk query in statement");
            }

            let table_columns = table_columns_by_name
                .get(&tbl_id)
                .expect(DB_STATE_INCORRECT);

            let mut selected_column_ids: Vec<u8> = if s_columns.is_empty() {
                table_columns
                    .values()
                    .into_iter()
                    .map(|d| d.col_id)
                    .collect()
            } else {
                s_columns
                    .iter()
                    .map(|c| table_columns.get(c).expect(DB_STATE_INCORRECT).col_id)
                    .collect()
            };

            selected_column_ids.sort();
            BQuery::SelectQuery {
                entity: QueryEntity {
                    ks_id: 0,
                    table_id: tbl_id,
                    operation: OperationId::SELECT,
                },
                fast_path,
                select_columns: selected_column_ids,
                conditions: Option::Some(conditions),
            }
        }
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            unique,
            if_not_exists,
        } => {
            println!("idx columns {:?}", columns);
            let (ks_name, tbl_name, ks_id, tbl_id) = parse_table_ks_name(
                &table_name.to_string().to_lowercase(),
                keyspaces_by_name,
                tables_by_name,
            )?;
            let table_columns = table_columns_by_name
                .get(&tbl_id)
                .expect(DB_STATE_INCORRECT);

            let mut columns_d: Vec<ColumnDescription> = vec![];
            let mut order: Option<Order> = Option::None;

            let mut vector_idx = false;

            for c in columns {
                match c.expr {
                    Expr::Identifier(ident) => {
                        let d = table_columns.get(&ident.value).unwrap();
                        if (d.data_type.vector) {
                            vector_idx = true;
                        }
                        order = if c.asc.is_some() {
                            Option::Some(Order::ASC)
                        } else {
                            Option::None
                        }; //TODO asc desc 
                        columns_d.push(d.clone());
                    }
                    _ => return sql_err!("unsupported index column"),
                }
            }

            let idx_type = if vector_idx {
                IdxType::VECTOR
            } else {
                IdxType::Range
            };

            BQuery::CreateIndex {
                idx_name: name.to_string().to_lowercase(),
                entity: QueryEntity {
                    ks_id: 0,
                    table_id: tbl_id,
                    operation: OperationId::CREATE_INDEX,
                },
                idx_type,
                columns: columns_d.iter().map(|c| c.col_id).collect(),
                order: order,
            }
        }
        _ => return sql_err!(BAD_QUERY),
    };

    println!("parsed {:?}", &parsed_query);
    Ok(parsed_query)
}

fn parse_table_ks_name(
    names: &String,
    keyspaces_by_name: &HashMap<String, u32>,
    tables_by_name: &HashMap<String, u32>,
) -> Result<(String, String, u32, u32)> {
    let ks_table: Vec<&str> = names.split('.').collect();

    if ks_table.len() != 2 {
        bail!("query should contain keyspace & table names")
    }

    let ks_name = String::from(*ks_table.get(0).unwrap());
    let table_name = String::from(*ks_table.get(1).unwrap());

    let ks_id: Option<&u32> = keyspaces_by_name.get(&ks_name);
    if ks_id.is_none() {
        bail!("keyspace {} not exist", table_name)
    }

    let tbl_id = tables_by_name.get(&table_name);
    if tbl_id.is_none() {
        bail!("table {} not exist", table_name)
    }

    Ok((ks_name, table_name, *ks_id.unwrap(), *tbl_id.unwrap()))
}

fn parse_selection(
    selection: &Expr,
    column_descriptions: &HashMap<String, ColumnDescription>,
) -> Result<Vec<WhereConditionsNode>> {
    let mut conditions: Vec<WhereConditionsNode> = Vec::new();
    match selection {
        Expr::BinaryOp { left, op, right } => {
            println!("Binary operation in WHERE: LEFT {:?}", left);
            println!("Binary operation in WHERE: OP {:?}", op);
            println!("Binary operation in WHERE: RIGHT {:?}", right);

            let and_op = match op {
                BinaryOperator::And => true,
                _ => false,
            };

            if (!and_op) {
                let col_name = match left.as_ref() {
                    Expr::Identifier(_ident) => {
                        let col_name = &_ident.value;
                        col_name.clone()
                    }
                    _ => return sql_err!("not supported selection"),
                };

                let operation: QueryConditionType = match op {
                    BinaryOperator::Eq => QueryConditionType::EQ,
                    BinaryOperator::Gt => QueryConditionType::GT,
                    BinaryOperator::Lt => QueryConditionType::LT,
                    BinaryOperator::LtEq => QueryConditionType::LTE,
                    BinaryOperator::GtEq => QueryConditionType::GTE,
                    _ => QueryConditionType::UNK,
                };
                let str_val = right.to_string();
                let placeholder = str_val.contains("?");

                let d = column_descriptions.get(&col_name);
                match d {
                    Some(c_d) => {
                        conditions.push(WhereConditionsNode {
                            col_id: c_d.col_id,
                            operator: operation,
                            values: vec![str_val],
                            placeholder: placeholder,
                        });
                    }
                    None => return sql_err!("inknown column name"),
                }
            } else {
                conditions.extend(parse_selection(left.as_ref(), column_descriptions)?);
                conditions.extend(parse_selection(right.as_ref(), column_descriptions)?);
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            println!("in list expr {:?}", expr);
            let col_name = match expr.as_ref() {
                Expr::Identifier(_ident) => _ident.value.clone(),
                _ => return sql_err!("no identifier"),
            };

            let mut values: Vec<String> = vec![];

            for i in list {
                match i {
                    Expr::Value(value) => match value {
                        Value::Number(s, _) => {
                            if s.contains("?") {
                                return sql_err!("placeholder not allowed for IN clause");
                            }
                            values.push(s.clone());
                        }
                        _ => return sql_err!(BAD_QUERY),
                    },
                    _ => return sql_err!(BAD_QUERY),
                }
            }
            let d = column_descriptions.get(&col_name);
            match d {
                Some(c_d) => {
                    conditions.push(WhereConditionsNode {
                        col_id: c_d.col_id,
                        operator: QueryConditionType::IN,
                        values: values,
                        placeholder: false,
                    });
                }
                None => return sql_err!("inknown column name"),
            }
        }
        _ => {}
    }

    Ok(conditions)
}

fn map_conditions(
    conditions: &Vec<WhereConditionsNode>,
    table_columns: &HashMap<u8, ColumnDescription>,
    prepared: bool,
) -> Result<Vec<QueryCondition>> {
    let mut cond_vec: Vec<QueryCondition> = vec![];
    for cond in conditions {
        let cond_type = &cond.operator;
        let cond_col = &cond.col_id;

        let res = table_columns.get(cond_col);
        if res.is_none() {
            return sql_err!(&format!("unknown column name {}", cond_col));
        }

        let d = res.unwrap();

        if matches!(cond_type, QueryConditionType::IN) {
            let mut parsed_values: Vec<Vec<u8>> = vec![];

            for v in &cond.values {
                if v.contains("?") {
                    bail!("placeholder for IN clause not allowed")
                }

                let values: Vec<&str> = v.split(",").collect();
                for val in values {
                    let (v, val_bytes) = parse_str_value(d.data_type.id, val, d.dimension)
                        .map_err(|e| anyhow!("bad value {}", val))?;
                    parsed_values.push(val_bytes);
                }
            }
            cond_vec.push(QueryCondition {
                c_type: QueryConditionType::IN,
                col_id: d.col_id,
                values: parsed_values,
                placeholder: false,
            });
        } else {
            let cond_val = cond.values.get(0).unwrap();
            let placeholder = cond_val.len() == 1 && cond_val.contains("?");
            if (placeholder && !prepared) {
                return sql_err!("placeholder in prepared statement");
            }

            if placeholder {
                cond_vec.push(QueryCondition {
                    c_type: cond_type.clone(),
                    col_id: d.col_id,
                    values: vec![],
                    placeholder: placeholder,
                });
            } else {
                let (v, val_bytes) = parse_str_value(d.data_type.id, &cond_val, d.dimension)
                    .map_err(|e| anyhow!("bad value {}", cond_val))?;
                cond_vec.push(QueryCondition {
                    c_type: cond_type.clone(),
                    col_id: d.col_id,
                    values: vec![val_bytes],
                    placeholder: false,
                });
            }
        }
    }
    Ok(cond_vec)
}

pub fn set_payload_size(data: &mut Vec<u8>, offset: usize, rows_count: u32) {
    data[4..8].copy_from_slice(&u32::to_le_bytes(offset as u32 - HEADER_SIZE));
    data[8..12].copy_from_slice(&u32::to_le_bytes(rows_count as u32));
}

pub fn empty_response(data: &mut Vec<u8>) -> usize {
    let mut offset: usize = 0;
    data[offset] = 0xAA;
    offset += 1;
    data[offset] = 0;
    offset += 1;
    data[offset] = 0;
    offset += 1;
    data[offset] = 0;
    offset += 1;
    data[offset..offset + 4].copy_from_slice(&u32::to_le_bytes(0)); //payload size
    offset += 4;

    offset
}

pub fn error_response(error: ErrorCode, msg: Option<String>, data: &mut Vec<u8>) -> usize {
    base_header(error, data);

    if let Some(msg) = msg {
        let str_bytes = msg.as_bytes();
        data[4..8].copy_from_slice(&u32::to_le_bytes(str_bytes.len() as u32));
        data[8..8 + str_bytes.len()].copy_from_slice(&str_bytes);
        8 + str_bytes.len()
    } else {
        8
    }
}

pub fn payload_response(error: ErrorCode, payload: Vec<u8>, data: &mut Vec<u8>) -> usize {
    base_header(error, data);
    let len = payload.len();
    data[4..8].copy_from_slice(&u32::to_le_bytes(len as u32));
    data[8..8 + len].copy_from_slice(&payload);
    8 + len
}

pub fn base_header(error: ErrorCode, data: &mut Vec<u8>) -> usize {
    data[0] = PROTOCOL_MAGIC;
    data[1] = PROTOCOL_VERSION;
    data[2] = error as u8;
    data[3] = 0; //reserved
    data[4..8].copy_from_slice(&u32::to_le_bytes(0 as u32));
    8
}

pub fn prepare_rows_response(
    col_names: &HashMap<u8, Vec<u8>>,
    select_columns: &Vec<ColumnDescription>,
    data: &mut Vec<u8>,
) -> usize {
    let mut offset: usize = 0;
    data[offset] = PROTOCOL_MAGIC;
    offset += 1;
    data[offset] = PROTOCOL_VERSION;
    offset += 1;
    data[offset] = ErrorCode::OK_OPCODE as u8;
    offset += 1;
    data[offset] = 0;
    offset += 1;

    data[offset..offset + 4].copy_from_slice(&u32::to_le_bytes(0)); //payload size
    offset += 4;

    data[offset..offset + 4].copy_from_slice(&u32::to_le_bytes(0 as u32)); //rows len
    offset += 4;
    data[offset] = select_columns.len() as u8;
    offset += 1;

    for c in select_columns {
        data[offset] = c.col_id;
        offset += 1;
        data[offset] = c.data_type.id;
        offset += 1;
        data[offset..offset + 2].copy_from_slice(&u16::to_le_bytes(c.dimension));
        offset += 2;

        let name_len = col_names.get(&c.col_id).unwrap().len() as u8;
        data[offset] = name_len;
        offset += 1;
        data[offset..offset + name_len as usize].copy_from_slice(col_names.get(&c.col_id).unwrap());
        offset += name_len as usize;
    }

    offset
}

pub fn write_row(
    row: &Vec<(u8, Vec<u8>)>,
    select_columns: &HashMap<u8, ColumnDescription>,
    data: &mut Vec<u8>,
    base_offset: usize,
) -> usize {
    let mut offset = base_offset;
    for (c_id, bytes) in row {
        data[offset] = *c_id;
        offset += 1;

        let d = select_columns.get(c_id).unwrap().data_type;
        let length: usize = bytes.len();
        if (d.vector || d.id < NUMERIC_ID_LIMIT || select_columns.get(c_id).unwrap().dimension > 0)
        {
            data[offset..offset + length].copy_from_slice(&bytes);
            offset += length;
        } else {
            data[offset..offset + 4].copy_from_slice(&u32::to_le_bytes(length as u32));
            offset += 4;
            data[offset..offset + length].copy_from_slice(&bytes);
            offset += length;
        }
    }
    offset
}

pub fn write_rows(
    rows: &Vec<Arc<Row>>,
    select_columns: &Vec<u8>,
    data: &mut Vec<u8>,
    base_offset: usize,
) -> usize {
    let mut offset = base_offset;

    if rows.is_empty() {
        return base_offset;
    }

    for r in rows {
        let row_offset = offset;

        /*
        offset += 1 ;
        let mut columns_count = 0;
        for c in &r.payload {
            if !select_columns.contains(&c.col_id) && c.value.len() == 0 {
                continue;
            }
            data[offset] = c.col_id;
            offset += 1;
            columns_count += 1;
        }
        data[row_offset] = columns_count as u8;
        */

        for c in &r.payload {
            if !select_columns.contains(&c.col_id) {
                continue;
            }
            data[offset] = c.col_id;
            offset += 1;

            let d = DATA_TYPES_MAP.get(&c.d_type).unwrap();

            let length: usize = c.value.len();
            if (d.vector || d.id < NUMERIC_ID_LIMIT) {
                data[offset..offset + length].copy_from_slice(&c.value);
                offset += length;
            } else {
                println!("write len {} at offset {}", c.value.len(), offset);
                data[offset..offset + 4].copy_from_slice(&u32::to_le_bytes(c.value.len() as u32));
                offset += 4;
                data[offset..offset + length].copy_from_slice(&c.value);
                offset += length;
            }
        }
    }

    offset
}

pub fn send_response(stream: &mut TcpStream, resp: QueryResult, out: &mut Vec<u8>) {
    //println!("resp {:?}", resp);
    match resp {
        QueryResult::Simple { err: code } => {
            let len = base_header(code, out);
            stream.write(&out[0..len]);
        }
        QueryResult::Error { err, msg } => {
            let len = error_response(err, Option::Some(msg), out);
            stream.write(&out[0..len]);
        }
        QueryResult::Plain { response: data } => {
            send_http_response(stream, data);
        }
        QueryResult::Full { data, len } => {
            stream.write(&data[0..len]);
        }
        QueryResult::Streaming { len } => {
            stream.write(&out[0..len]);
        }
        QueryResult::Payload { data } => {
            let len = payload_response(ErrorCode::OK_OPCODE, data, out);
            stream.write(&out[0..len]);
        }
    };
}

pub fn send_http_response(stream: &mut TcpStream, body: Vec<u8>) {
    println!("body length {}", body.len());

    let str = String::from_utf8(body).unwrap();
    println!("str length {}", str.len());
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        str.len(),
        str
    );
    stream.write(resp.as_bytes());
}

pub fn create_prepared_columns_header(
    header_columns: &Vec<u8>,
    columns_d: &HashMap<u8, ColumnDescription>,
    col_names: &HashMap<u8, Vec<u8>>,
    data: &mut Vec<u8>,
) -> usize {
    base_header(ErrorCode::OK_OPCODE, data);
    data[8..10].copy_from_slice(&u16::to_le_bytes(0)); //rows count
    data[11] = header_columns.len() as u8;

    let mut offset: usize = 8;
    for c_id in header_columns {
        let c = columns_d.get(c_id).unwrap();
        data[offset] = c.col_id;
        offset += 1;
        data[offset] = c.data_type.id;
        offset += 1;
        data[offset] = c.flags; //set if zero
        offset += 1;
        data[offset..offset + 2].copy_from_slice(&u16::to_le_bytes(c.dimension));
        offset += 2;

        let name = col_names.get(&c.col_id).unwrap();
        let name_len = name.len();
        data[offset] = name_len as u8;
        offset += 1;
        data[offset..offset + name_len as usize].copy_from_slice(col_names.get(&c.col_id).unwrap());
        offset += name_len as usize;
    }
    offset
}

pub fn parse_header(msg_length: usize, buffer: &[u8]) -> Header {
    let magic = buffer[0];
    let version = buffer[1];
    let cmd = buffer[2];
    let reserved = buffer[3];
    let length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
    let header = Header {
        magic,
        version,
        cmd,
        length,
    };
    header
}

//curl -X GET "http://127.0.0.1:8081/query?select%20from%20ks1.tbl1%20(vec1)"
pub fn parse_http(bytes: &[u8]) -> Result<String> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = Request::new(&mut headers);

    let payload = req.parse(bytes)?;
    let s: String = match payload {
        Status::Complete(p) => match req.path {
            Some(p) => {
                let start = p.find("query");
                match start {
                    Some(q_s) => {
                        let query = &req.path.unwrap()[q_s + 6..p.len()];
                        String::from(query)
                    }
                    None => bail!("bad query string"),
                }
            }
            None => bail!("bad query string"),
        },
        Status::Partial => {
            bail!("unknown error")
        }
    };

    Ok(s)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, hash::Hash, io::Read};

    use sqlparser::parser::Parser;

    use crate::{
        column_descr,
        data::{index::IdxType, segment::ColumnDescription},
        model::data_type::{
            FLOAT_VECTOR, INT, INT_VECTOR, LONG, LONG_VECTOR, TIMESTAMP, map_data_type,
            parse_str_value,
        },
        protocol::{BQuery, CustomSqlDialect, OperationId, QueryConditionType, parse_plain},
    };

    fn create_column(
        name: String,
        data_type: u8,
        flags: u8,
        col_id: u8,
        dimension: u16,
    ) -> ColumnDescription {
        column_descr!(
            col_id,
            flags,
            dimension,
            map_data_type(data_type),
            "name".as_bytes().to_vec()
        )
    }

    #[test]
    fn test_sql_parser() {
        let dialect = CustomSqlDialect {};
        let ast = Parser::parse_sql(&dialect, "create index v_idx on ks1.table1(id)").unwrap();
        let ast = Parser::parse_sql(
            &dialect,
            "select id, sim_cosine(vec, '12,12') from ks1.table1 ",
        )
        .unwrap();
        let ast = Parser::parse_sql(
            &dialect,
            "select * from ks1.table1 where id = 10 and sec = 20 and th > 30",
        )
        .unwrap();
        let ast =
            Parser::parse_sql(&dialect, "select id from ks1.table1 where id in (10,20)").unwrap();
        let ast = Parser::parse_sql(
            &dialect,
            "insert into ks1.table1 (pk, ts, vec) values (10, ?, '2,3,4,5')",
        )
        .unwrap();
        let ast =
            Parser::parse_sql(&dialect, "select id from ks1.table1 where id = ? limit 10").unwrap();
        let ast = Parser::parse_sql(
            &dialect,
            "select id from ks1.table1 where id in (10,20) AND ts >= 10",
        )
        .unwrap();
        let ast = Parser::parse_sql(
            &dialect,
            "select id from ks1.table1 where id in (10,20) AND s_id in (100,200) AND ts >= 10",
        )
        .unwrap();
        let ast = Parser::parse_sql(&dialect, "create table ks1.table1 (id long, ts timestamp not null, vec long_vector,  primary key(id, ts))").unwrap();
        let ast = Parser::parse_sql(&dialect, "create table ks1.table1 (id long, ts timestamp not null, vec long_vector,  primary key(id, ts)) WITH (range_idx=true)").unwrap();
        println!("ast {:?}", ast);
    }

    #[test]
    fn test_sql_create_keyspace() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        let query = parse_plain(
            "create keyspace ks1".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::DdlQuery {
                entity,
                keyspace_name,
                table_name,
                columns_to_add,
                columns_to_drop,
                primary_key_size,
                with_range_idx: range_idx,
            } => {
                assert_eq!(entity.operation, OperationId::CREATE_KEYSPACE);
                assert_eq!(entity.ks_id, 0);
                assert_eq!(entity.table_id, 0);
                assert!(keyspace_name.is_some());
                assert_eq!(
                    "ks1".to_string(),
                    String::from_utf8(keyspace_name.unwrap()).unwrap()
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_create_table_single_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);

        let query = parse_plain(
            "create table ks1.table1 (id long, ts int, vec long_vector[16],  primary key(id))"
                .to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::DdlQuery {
                entity,
                keyspace_name,
                table_name,
                columns_to_add,
                columns_to_drop,
                primary_key_size,
                with_range_idx,
            } => {
                assert_eq!(entity.operation, OperationId::CREATE_TABLE);
                assert_eq!(entity.ks_id, 1);
                assert_eq!(entity.table_id, 0);
                assert!(table_name.is_some());
                assert_eq!(
                    "table1".to_string(),
                    String::from_utf8(table_name.unwrap()).unwrap()
                );
                assert_eq!(primary_key_size, 1);
                assert!(!with_range_idx);
                assert!(columns_to_drop.is_none());
                assert!(columns_to_add.is_some());
                assert_eq!(columns_to_add.as_ref().unwrap().len(), 3);

                let col1 = columns_to_add.as_ref().unwrap().get(0).unwrap();
                assert_eq!(col1.col_id, 0);
                assert_eq!(col1.data_type.id, LONG.id);
                assert_eq!(col1.dimension, 0);
                assert_eq!(col1.flags, 0x1);
                assert_eq!(col1.name.to_vec(), "id".as_bytes().to_vec());

                let col2 = columns_to_add.as_ref().unwrap().get(1).unwrap();
                assert_eq!(col2.col_id, 1);
                assert_eq!(col2.data_type.id, INT.id);
                assert_eq!(col2.dimension, 0);
                assert_eq!(col2.flags, 0x0);
                assert_eq!(col2.name.to_vec(), "ts".as_bytes().to_vec());

                let col3 = columns_to_add.as_ref().unwrap().get(2).unwrap();
                assert_eq!(col3.col_id, 2);
                assert_eq!(col3.data_type.id, LONG_VECTOR.id);
                assert_eq!(col3.dimension, 16);
                assert_eq!(col3.flags, 0x0);
                assert_eq!(col3.name.to_vec(), "vec".as_bytes().to_vec());
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_create_table_composite_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);

        let query = parse_plain(
            "create table ks1.table1 (id long, ts int, vec long_vector[16],  primary key(id, ts)) with (range_idx=true)".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id).unwrap();

        match query {
            BQuery::DdlQuery {
                entity,
                keyspace_name,
                table_name,
                columns_to_add,
                columns_to_drop,
                primary_key_size,
                with_range_idx,
            } => {
                assert_eq!(entity.operation, OperationId::CREATE_TABLE);
                assert_eq!(entity.ks_id, 1);
                assert_eq!(entity.table_id, 0);
                assert!(table_name.is_some());
                assert_eq!(
                    "table1".to_string(),
                    String::from_utf8(table_name.unwrap()).unwrap()
                );
                assert_eq!(primary_key_size, 2);
                assert!(with_range_idx);
                assert!(columns_to_drop.is_none());
                assert!(columns_to_add.is_some());
                assert_eq!(columns_to_add.as_ref().unwrap().len(), 3);

                let col1 = columns_to_add.as_ref().unwrap().get(0).unwrap();
                assert_eq!(col1.col_id, 0);
                assert_eq!(col1.data_type.id, LONG.id);
                assert_eq!(col1.dimension, 0);
                assert_eq!(col1.flags, 0x1);
                assert_eq!(col1.name.to_vec(), "id".as_bytes().to_vec());

                let col2 = columns_to_add.as_ref().unwrap().get(1).unwrap();
                assert_eq!(col2.col_id, 1);
                assert_eq!(col2.data_type.id, INT.id);
                assert_eq!(col2.dimension, 0);
                assert_eq!(col2.flags, 0x1);
                assert_eq!(col2.name.to_vec(), "ts".as_bytes().to_vec());

                let col3 = columns_to_add.as_ref().unwrap().get(2).unwrap();
                assert_eq!(col3.col_id, 2);
                assert_eq!(col3.data_type.id, LONG_VECTOR.id);
                assert_eq!(col3.dimension, 16);
                assert_eq!(col3.flags, 0x0);
                assert_eq!(col3.name.to_vec(), "vec".as_bytes().to_vec());
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_select_by_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(
            1,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "select * from ks1.table1 where id = 10".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::SELECT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(select_columns.len(), 2);
                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 1);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![10, 0, 0, 0, 0, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_prepared_select_by_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(
            1,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "select vec from ks1.table1 where id = ? and vec > ?".to_string(),
            true,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::SELECT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(select_columns.len(), 1);
                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 2);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert!(
                    conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .is_empty()
                );
                assert!(matches!(
                    conditions.as_ref().unwrap().get(1).unwrap().c_type,
                    QueryConditionType::GT
                ));
                assert_eq!(conditions.as_ref().unwrap().get(1).unwrap().col_id, 1);
                assert!(
                    conditions
                        .as_ref()
                        .unwrap()
                        .get(1)
                        .unwrap()
                        .values
                        .is_empty()
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_select_by_composite_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(1, create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0));
        table_columns.insert(
            2,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "ts".to_string(),
            create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "select vec from ks1.table1 where id = 10 and ts = 33".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::SELECT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(select_columns.len(), 1);
                assert_eq!(*select_columns.get(0).unwrap(), 2);
                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 2);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert!(matches!(
                    conditions.as_ref().unwrap().get(1).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert_eq!(conditions.as_ref().unwrap().get(1).unwrap().col_id, 1);
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![10, 0, 0, 0, 0, 0, 0, 0]
                );
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(1)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![33, 0, 0, 0, 0, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_select_by_range() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(1, create_column("ts".to_string(), TIMESTAMP.id, 0x0, 1, 0));
        table_columns.insert(
            2,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "ts".to_string(),
            create_column("ts".to_string(), TIMESTAMP.id, 0x0, 1, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "select vec from ks1.table1 where id = 10 and ts > 33".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::SELECT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(select_columns.len(), 1);
                assert_eq!(*select_columns.get(0).unwrap(), 2);
                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 2);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert!(matches!(
                    conditions.as_ref().unwrap().get(1).unwrap().c_type,
                    QueryConditionType::GT
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert_eq!(conditions.as_ref().unwrap().get(1).unwrap().col_id, 1);
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![10, 0, 0, 0, 0, 0, 0, 0]
                );
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(1)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![33, 0, 0, 0, 0, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_select_in_clause() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(1, create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0));
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "ts".to_string(),
            create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "select * from ks1.table1 where id in (10,20) and ts = 100".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::SelectQuery {
                entity,
                select_columns,
                fast_path,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::SELECT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(select_columns.len(), 2);
                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 2);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::IN
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().values.len(), 2);
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![10, 0, 0, 0, 0, 0, 0, 0]
                );
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(1)
                        .unwrap(),
                    vec![20, 0, 0, 0, 0, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_insert() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(
            1,
            create_column("vec".to_string(), INT_VECTOR.id, 0x0, 1, 4),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), INT_VECTOR.id, 0x0, 1, 4),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "insert into ks1.table1 (id, vec) values (100, '11,22,33,44')".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::UpdateQuery {
                entity,
                input,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::INSERT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(input.len(), 2);

                assert_eq!(input.get(0).unwrap().col_id, 0);
                assert_eq!(input.get(0).unwrap().d_type, LONG.id);
                assert_eq!(input.get(0).unwrap().dimension, 0);
                assert_eq!(input.get(0).unwrap().flags, 0x1);
                assert_eq!(input.get(0).unwrap().num_val, 100);
                assert_eq!(input.get(0).unwrap().value, vec![100, 0, 0, 0, 0, 0, 0, 0]);

                assert_eq!(input.get(1).unwrap().col_id, 1);
                assert_eq!(input.get(1).unwrap().d_type, INT_VECTOR.id);
                assert_eq!(input.get(1).unwrap().dimension, 4);
                assert_eq!(input.get(1).unwrap().flags, 0x0);
                assert_eq!(
                    input.get(1).unwrap().value,
                    vec![11, 0, 0, 0, 22, 0, 0, 0, 33, 0, 0, 0, 44, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_prepared_insert() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(
            1,
            create_column("vec".to_string(), INT_VECTOR.id, 0x0, 1, 4),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), INT_VECTOR.id, 0x0, 1, 4),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "insert into ks1.table1 (id, vec) values (?, ?)".to_string(),
            true,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::UpdateQuery {
                entity,
                input,
                conditions,
            } => {
                assert_eq!(entity.operation, OperationId::INSERT);
                assert_eq!(entity.table_id, 10);
                assert_eq!(input.len(), 2);

                assert_eq!(input.get(0).unwrap().col_id, 0);
                assert_eq!(input.get(0).unwrap().d_type, LONG.id);
                assert_eq!(input.get(0).unwrap().dimension, 0);
                assert_eq!(input.get(0).unwrap().flags, 0x1);
                assert!(input.get(0).unwrap().value.is_empty());

                assert_eq!(input.get(1).unwrap().col_id, 1);
                assert_eq!(input.get(1).unwrap().d_type, INT_VECTOR.id);
                assert_eq!(input.get(1).unwrap().dimension, 4);
                assert_eq!(input.get(1).unwrap().flags, 0x0);
                assert!(input.get(1).unwrap().value.is_empty());
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_delete_by_pk() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(
            1,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 1, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "delete from ks1.table1 where id = 10".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::DeleteQuery { entity, conditions } => {
                assert_eq!(entity.operation, OperationId::DELETE);
                assert_eq!(entity.table_id, 10);

                assert!(conditions.is_some());
                assert_eq!(conditions.as_ref().unwrap().len(), 1);
                assert!(matches!(
                    conditions.as_ref().unwrap().get(0).unwrap().c_type,
                    QueryConditionType::EQ
                ));
                assert_eq!(conditions.as_ref().unwrap().get(0).unwrap().col_id, 0);
                assert_eq!(
                    *conditions
                        .as_ref()
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .values
                        .get(0)
                        .unwrap(),
                    vec![10, 0, 0, 0, 0, 0, 0, 0]
                );
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_create_idx_range_single() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(1, create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0));
        table_columns.insert(
            2,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "ts".to_string(),
            create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "create index idx1 on ks1.table1 (ts)".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::CreateIndex {
                entity,
                idx_name,
                idx_type,
                columns,
                order,
            } => {
                assert_eq!(entity.operation, OperationId::CREATE_INDEX);
                assert_eq!(entity.ks_id, 0);
                assert_eq!(entity.table_id, 10);
                assert!(matches!(idx_type, IdxType::Range));
                assert_eq!(columns.len(), 1);
                assert_eq!(*columns.get(0).unwrap(), 1);
                assert_eq!(idx_name, "idx1".to_string());
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }

    #[test]
    fn test_sql_create_idx_vector() {
        let mut keyspaces_by_name: HashMap<String, u32> = HashMap::new();
        let mut tables_by_name: HashMap<String, u32> = HashMap::new();
        let mut table_columns_by_name: HashMap<u32, HashMap<String, ColumnDescription>> =
            HashMap::new();
        let mut table_columns_by_id: HashMap<u32, HashMap<u8, ColumnDescription>> = HashMap::new();

        keyspaces_by_name.insert("ks1".to_string(), 1);
        tables_by_name.insert("table1".to_string(), 10);

        let mut table_columns: HashMap<u8, ColumnDescription> = HashMap::new();
        table_columns.insert(0, create_column("id".to_string(), LONG.id, 0x1, 0, 0));
        table_columns.insert(1, create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0));
        table_columns.insert(
            2,
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_id.insert(10, table_columns.clone());

        let mut columns_by_name: HashMap<String, ColumnDescription> = HashMap::new();
        columns_by_name.insert(
            "id".to_string(),
            create_column("id".to_string(), LONG.id, 0x1, 0, 0),
        );
        columns_by_name.insert(
            "ts".to_string(),
            create_column("ts".to_string(), TIMESTAMP.id, 0x1, 1, 0),
        );
        columns_by_name.insert(
            "vec".to_string(),
            create_column("vec".to_string(), FLOAT_VECTOR.id, 0x0, 2, 8),
        );
        table_columns_by_name.insert(10, columns_by_name.clone());

        let query = parse_plain(
            "create index idx1 on ks1.table1 (vec)".to_string(),
            false,
            &keyspaces_by_name,
            &tables_by_name,
            &table_columns_by_name,
            &table_columns_by_id,
        )
        .unwrap();

        match query {
            BQuery::CreateIndex {
                entity,
                idx_name,
                idx_type,
                columns,
                order,
            } => {
                assert_eq!(entity.operation, OperationId::CREATE_INDEX);
                assert_eq!(entity.ks_id, 0);
                assert_eq!(entity.table_id, 10);
                assert!(matches!(idx_type, IdxType::VECTOR));
                assert_eq!(columns.len(), 1);
                assert_eq!(*columns.get(0).unwrap(), 2);
                assert_eq!(idx_name, "idx1".to_string());
            }
            _ => {
                assert!(false, "wrong parsed query type {:?}", query);
            }
        }
    }
}
