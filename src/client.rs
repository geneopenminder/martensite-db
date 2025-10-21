// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use anyhow::{Context, Result, bail, ensure};
use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// **********************************
/// Client source code independent
/// could't be copied to another rust project without additional deps
/// **********************************

/// PROTOCOL
/// *** All values always LITTLE_ENDIAN ***
/// Headers for all message types
/// Request header                          Response header       
/// --------------------                    ----------------------
/// offset      | value                     offset      | value
/// 0x00        | 0xAA -- magic             0x00        | 0xAA -- magic
/// 0x01        | 0x01 -- protocol version  0x01        | 0x01 -- protocol version
/// 0x02        | 0xXX -- cmd               0x02        | 0xXX -- error code
/// 0x03        | reserved                  0x03        | reserved
/// 0x04-0x07   | payload length            0x04-0x07   | payload length
/// 0x08 -- 0x..| payload bytes             0x08 -- 0x..| payload bytes / UTF-8 error message for error code > 0
/// --------------------
///
///
/// Create session -- first message for new connection
/// Request                                 Response       
/// --------------------                    ----------------------
/// offset      | value                     offset      | value
/// 0x00        | 0xAA -- magic              0x00       | 0xAA -- magic
/// 0x01        | 0x01 -- protocol version  0x01        | 0x01 -- protocol version
/// 0x02        | 0x0C -- cmd               0x02        | 0xXX -- error code
/// 0x03        | reserved                  0x03        | reserved
/// 0x04-0x07   | 0                         0x04-0x07   | 0
/// --------------------
///
///
/// Create prepared statement
/// Request                                 Response       
/// --------------------                    ----------------------
/// offset      | value                     offset      | value
/// 0x00        | 0xAA -- magic             0x00        | 0xAA -- magic
/// 0x01        | 0x01 -- protocol version  0x01        | 0x01 -- protocol version
/// 0x02        | 0x02 -- cmd               0x02        | 0xXX -- error code
/// 0x03        | reserved                  0x03        | reserved
/// 0x04-0x07   | query length in bytes     0x04-0x07   | 5
/// 0x08-0xXX   | query string UTF-8        0x08-0x0B   | prepared statement ID u32
///             |                           0x0C        | values/placeholders count u8
///
///
/// Execute prepared statement
/// Request
//// --------------------
/// offset      | value                    
/// 0x00        | 0xAA -- magic            
/// 0x01        | 0x01 -- protocol version
/// 0x02        | 0x03 -- cmd
/// 0x03        | reserved
/// 0x04-0x07   | payload length
/// 0x08-0x0B   | prepared statement ID u32
/// 0x0B-0x0E   | values count = equals placeholders count. Batch requests not supported yet
///
/// value record  format
/// value length in bytes - u32
/// value bytes *LITTLE_ENDIAN* as all other values
///
/// example payload for query insert into ks.tbl (pk long, vector long_vector[4], text) values (?,?,?) -- insert into ks.tbl (pk long, vector long_vector[4], text) values (100, [1,2,3,4], 'abcd')
/// values/placeholder count - 3
/// 0x00-0x03   | 8 - length of u64 -- value idx 0
/// 0x04-0x08   | 100
/// 0x08-0x0B   | 32 - length of long_vector with dimenstion 4 -- 4 * 8 -- value idx 1
/// 0x0B-0xXX   | 1,2,3,4 as [u8] with little endian for every u64 value
/// 0xXX-0xX2   | 4 - length of UTF-8 string -- values idx 3
/// 0x0X2 -...  | 'abcd'
///
/// Response -- rows - the same as for 'Excecute plain text query with rows result for SELECT' for any king of prepared SELECT/INSERT/DELETE
///
/// Excecute plain text query for for DDL/UPDATE/DELETE -- no rows response
/// Request                                 Response       
/// --------------------                    ----------------------
/// offset      | value                     offset      | value
/// 0x00        | 0xAA -- magic             0x00        | 0xAA -- magic
/// 0x01        | 0x01 -- protocol version  0x01        | 0x01 -- protocol version
/// 0x02        | 0x01 -- cmd               0x02        | 0xXX -- error code
/// 0x03        | reserved                  0x03        | reserved
/// 0x04-0x07   | query length in bytes     0x04-0x07   | 0 for OK, err message length for error code > 0
/// 0x08-0xXX   | query string UTF-8        0x08-0xXX   | response types -- error message UTF-8 string if error code > 0
/// --------------------
///
///
/// Excecute plain text query with rows result for SELECT
/// Request                                 Response       
/// --------------------                    ----------------------
/// offset      | value                     offset      | value
/// 0x00        | 0xAA -- magic             0x00        | 0xAA -- magic
/// 0x01        | 0x01 -- protocol version  0x01        | 0x01 -- protocol version
/// 0x02        | 0x01 -- cmd               0x02        | 0xXX -- error code
/// 0x03        | reserved                  0x03        | reserved
/// 0x04-0x07   | query length in bytes     0x04-0x07   | 0 for OK, err message length for error code > 0
/// 0x08-0xXX   | query string UTF-8        0x08-0xXX   | rows
/// --------------------
///
/// Rows response format
/// -------------------
/// offset
/// 0x00-x03    | rows count u32
///
/// columns header section -- header for all rows -- for every row every column exists - NULL values not supported yet
/// 0x04        | columns count u8
/// for every c in 0..columns count {
/// relative_offset | val
///     0x00        | column id u8
///     0x01        | data type u8
///     0x02-0x03   | dimension u16
///     0x04        | column name length
///     0x05-0xXX   | columns name UTF-8 bytes
/// }
/// rows section
/// for every row in 0..rows count {
///     for every column in 0..columns count {
///     relative_offset | val
///         0x00        | column id u8
///
///         if data type single num or vectror with known dimenstion {
///         relative_offset | val
///             0x00        | column id u8
///             0xXX        | value bytes
///         } else { variable length val - string/blob
///         relative_offset | val
///             0x00        | column id u8
///             0x01-0x03   | value size u32
///             0x04-0xXX   | value bytes
///         }
///     }
/// }
const PROTOCOL_MAGIC: u8 = 0xAA;
const PROTOCOL_VERSION: u8 = 0x1;

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum Command {
    PlainQuery = 0x01,
    CreatePrepared = 0x02,
    ExecPrepared = 0x03,
    CreateSession = 0x0C,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
enum ErrorCode {
    OK_OPCODE = 0x00,
    SYSTEM_UNKNOWN = 0x01,
    SYSTEM_UNKNOWN_PROTOCOL_VERSION = 0x02,
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
    SQL_UNKNOWN_DATA_TYPE = 0x34,
    SQL_NOT_FOUND = 0x35,
    SQL_UNSUPPORTED = 0x36,
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum DataTypeId {
    BYTE = 0x1,
    SHORT = 0x2,
    INT = 0x3,
    LONG = 0x4,
    TIMESTAMP = 0x5,
    FLOAT = 0x8,
    DOUBLE = 0x9,
    BYTE_VECTOR = 0x10,
    SHORT_VECTOR = 0x11,
    INT_VECTOR = 0x12,
    LONG_VECTOR = 0x13,
    FLOAT_VECTOR = 0x14,
    DOUBLE_VECTOR = 0x15,
    VARCHAR = 0x20,
    JSON = 0x21,
    BLOB = 0x22,

    UNKNOWN = 0xFF,
}

pub static NUMERIC_ID_LIMIT: u8 = 10;

#[derive(Debug)]
pub struct MDbClient {
    stream: TcpStream,
}

#[derive(Debug, Clone)]
struct MDbHeader {
    pub magic: u8,
    pub version: u8,
    pub err_code: u8,
    pub length: u32,
}

#[derive(Debug)]
pub struct MDbPrepStatement {
    st_id: u32,
    columns_count: u8,
    values: Vec<(u8, Vec<u8>)>,
}

impl MDbPrepStatement {
    pub fn set_byte(&mut self, index: usize, val: u8) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BYTE as u8, vec![val]);
        Ok(())
    }

    pub fn set_short(&mut self, index: usize, val: u16) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BYTE as u8, u16::to_le_bytes(val).to_vec());
        Ok(())
    }

    pub fn set_int(&mut self, index: usize, val: u32) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BYTE as u8, u32::to_le_bytes(val).to_vec());
        Ok(())
    }

    pub fn set_long(&mut self, index: usize, val: u64) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BYTE as u8, u64::to_le_bytes(val).to_vec());
        Ok(())
    }

    pub fn set_byte_vector(&mut self, index: usize, val: Vec<u8>) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BYTE as u8, val);
        Ok(())
    }

    pub fn set_long_vector(&mut self, index: usize, val: Vec<u64>) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        let mut bytes: Vec<u8> = Vec::with_capacity(val.len() * 8);
        for v in val {
            bytes.extend(u64::to_le_bytes(v));
        }
        self.values[index] = (DataTypeId::BYTE as u8, bytes);
        Ok(())
    }

    pub fn set_blob(&mut self, index: usize, val: Vec<u8>) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::BLOB as u8, val);
        Ok(())
    }

    pub fn set_text(&mut self, index: usize, val: &String) -> Result<()> {
        ensure!(index < self.values.len(), "incorrect index");
        self.values[index] = (DataTypeId::VARCHAR as u8, val.as_bytes().to_vec());
        Ok(())
    }
}

#[derive(Debug)]
pub struct Row {
    columns: HashMap<u8, ColumnDescription>,
    values: HashMap<String, ColumnVal>,
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "row:")?;
        for (k, v) in &self.values {
            let c_d = self.columns.get(&v.col_id);
            match c_d {
                Some(d) => {
                    let str_vl = value_to_string(&v.value, d.data_type);
                    match str_vl {
                        Ok(parsed) => {
                            writeln!(f, "{} : {}", k, parsed)?;
                        }
                        Err(_) => {
                            writeln!(f, "{} : bad value", k)?;
                        }
                    }
                }
                None => {
                    writeln!(f, "unknown data type")?;
                }
            }
        }
        writeln!(f, "\'")
    }
}

#[derive(Debug)]
pub struct ColumnVal {
    col_id: u8,
    value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ColumnDescription {
    pub dimension: u16,
    pub data_type: u8,
    pub col_id: u8,
    pub name: String,
}

fn parse_header(buffer: &[u8]) -> Result<MDbHeader> {
    let magic = buffer[0];
    let version = buffer[1];
    let err = buffer[2];
    let length = u32::from_le_bytes(buffer[4..8].try_into()?);
    let header = MDbHeader {
        magic,
        version,
        err_code: err,
        length,
    };
    Ok(header)
}

fn header(buf: &mut [u8], command: Command) {
    buf[0] = PROTOCOL_MAGIC;
    buf[1] = PROTOCOL_VERSION;
    buf[2] = command as u8;
    buf[3] = 0;
}

impl MDbClient {
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).context("failed to connect")?;
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

        let mut client = MDbClient { stream };
        client.create_session()?;
        Ok(client)
    }

    pub fn create_session(&mut self) -> Result<()> {
        let mut create_session: [u8; 8] = [0; 8];
        header(&mut create_session, Command::CreateSession);
        create_session[4..8].copy_from_slice(&u32::to_le_bytes(0));

        self.stream
            .write_all(&create_session)
            .context("failed to create session")?;
        self.stream.flush()?;

        let size = self.stream.read(&mut create_session)?;
        ensure!(size == 8, "protocol error");

        let header = parse_header(&create_session)?;
        ensure!(
            header.err_code == ErrorCode::OK_OPCODE as u8,
            "error code {}",
            header.err_code
        );

        Ok(())
    }

    fn check_error_code(&mut self, header: &MDbHeader) -> Result<()> {
        if header.err_code != ErrorCode::OK_OPCODE as u8 {
            if header.length != 0 {
                //error message
                let mut msg = vec![0u8; header.length as usize];
                self.read_exact(&mut msg)?;
                bail!(String::from_utf8(msg)?)
            } else {
                bail!("error {}", header.err_code)
            }
        }
        Ok(())
    }

    pub fn create_prepared(&mut self, query: &String) -> Result<MDbPrepStatement> {
        let bytes = query.as_bytes();
        let mut buf: Vec<u8> = vec![0u8; 8];
        header(&mut buf, Command::CreatePrepared);
        buf[4..8].copy_from_slice(&u32::to_le_bytes(bytes.len() as u32));
        buf.extend_from_slice(bytes);
        self.stream.write_all(&buf)?;

        let header = self.read_response_header()?;
        self.check_error_code(&header)?;

        let mut resp = vec![0u8; header.length as usize];
        self.read_exact(&mut resp)?;

        let statement_id: u32 = u32::from_le_bytes(resp[0..4].try_into()?);
        let columns_count = resp[4];

        let mut values: Vec<(u8, Vec<u8>)> = vec![];
        for i in 0..columns_count {
            values.push((0, vec![]));
        }

        Ok(MDbPrepStatement {
            st_id: statement_id,
            columns_count: columns_count,
            values,
        })
    }

    pub fn execute_plain_qeury(&mut self, query: &String) -> Result<u8> {
        let bytes = query.as_bytes();
        let mut buf: Vec<u8> = vec![0u8; 8];
        header(&mut buf, Command::PlainQuery);
        buf[4..8].copy_from_slice(&u32::to_le_bytes(bytes.len() as u32));
        buf.extend_from_slice(bytes);
        self.stream.write_all(&buf)?;

        let header = self.read_response_header()?;
        self.check_error_code(&header)?;
        Ok(header.err_code)
    }

    pub fn execute_prepared(&mut self, query: &MDbPrepStatement) -> Result<Vec<Row>> {
        let mut buf: Vec<u8> = vec![0u8; 8];
        header(&mut buf, Command::ExecPrepared);

        let len: usize = query.values.iter().map(|(d, v)| v.len()).sum();
        let mut payload: Vec<u8> = Vec::with_capacity(len * 2);

        payload.extend(u32::to_le_bytes(query.st_id));
        payload.extend(u32::to_le_bytes(query.values.len() as u32));

        for (d_type, v) in &query.values {
            if *d_type == DataTypeId::BLOB as u8
                || *d_type == DataTypeId::VARCHAR as u8
                || *d_type == DataTypeId::JSON as u8
            {
                payload.extend_from_slice(&u32::to_le_bytes(v.len() as u32));
                payload.extend_from_slice(&v);
            } else {
                payload.extend_from_slice(&v);
            }
        }

        buf[4..8].copy_from_slice(&u32::to_le_bytes(payload.len() as u32));
        buf.extend_from_slice(&payload);

        self.stream.write_all(&buf)?;

        let header = self.read_response_header()?;
        self.check_error_code(&header)?;

        return self.parse_result(&header);
    }

    pub fn execute_plain_qeury_with_result(&mut self, query: &String) -> Result<Vec<Row>> {
        let bytes = query.as_bytes();
        let mut buf: Vec<u8> = vec![0u8; 8];
        header(&mut buf, Command::PlainQuery);
        buf[4..8].copy_from_slice(&u32::to_le_bytes(bytes.len() as u32));
        buf.extend_from_slice(bytes);
        self.stream.write_all(&buf)?;

        let header = self.read_response_header()?;
        self.check_error_code(&header)?;

        return self.parse_result(&header);
    }

    fn parse_result(&mut self, header: &MDbHeader) -> Result<Vec<Row>> {
        if header.length == 0 {
            return Ok(vec![]);
        }

        let mut payload = vec![0u8; header.length as usize];
        self.read_exact(&mut payload)?;

        let rows_count = u32::from_le_bytes(payload[0..4].try_into()?);

        if rows_count == 0 {
            return Ok(vec![]);
        }

        let mut offset: usize = 4;

        ensure!(offset < header.length as usize, "bad message length");
        let columns_count = payload[offset];
        offset += 1;

        let mut result: Vec<Row> = vec![];
        let mut columns: HashMap<u8, ColumnDescription> = HashMap::new();

        for i in 0..columns_count {
            ensure!(offset + 4 < header.length as usize, "bad message length");
            let col_id = payload[offset];
            offset += 1;
            let data_type = payload[offset];
            offset += 1;

            let dimension = u16::from_le_bytes(payload[offset..offset + 2].try_into()?);
            offset += 2;
            let name_len: usize = payload[offset] as usize;
            offset += 1;
            ensure!(
                offset + name_len < header.length as usize,
                "bad message length"
            );
            let name = String::from_utf8(payload[offset..offset + name_len].to_vec())?;
            offset += name_len;
            columns.insert(
                col_id,
                ColumnDescription {
                    dimension,
                    data_type,
                    col_id,
                    name,
                },
            );
        }

        for i in 0..rows_count {
            ensure!(offset + 1 < header.length as usize, "bad message length");
            let mut values: HashMap<String, ColumnVal> = HashMap::new();

            for c_id in 0..columns_count {
                let col_id = payload[offset];
                offset += 1;

                let d = columns.get(&col_id);
                match d {
                    Some(c_d) => {
                        if c_d.data_type < NUMERIC_ID_LIMIT || c_d.dimension > 0 {
                            let size = calculate_value_size(c_d.data_type, c_d.dimension);
                            ensure!(offset < header.length as usize, "bad message length");
                            values.insert(
                                c_d.name.clone(),
                                ColumnVal {
                                    col_id: col_id,
                                    value: payload[offset..offset + size].to_vec(),
                                },
                            );
                            offset += size;
                        } else {
                            let size = u32::from_le_bytes(payload[offset..offset + 4].try_into()?)
                                as usize;
                            ensure!(offset < header.length as usize, "bad message length");
                            values.insert(
                                c_d.name.clone(),
                                ColumnVal {
                                    col_id: col_id,
                                    value: payload[offset..offset + size].to_vec(),
                                },
                            );
                            offset += size;
                        }
                    }
                    None => bail!("bad format"),
                }
            }
            result.push(Row {
                columns: columns.clone(),
                values: values,
            });
        }

        Ok(result)
    }

    fn read_response_header(&mut self) -> Result<MDbHeader> {
        let mut receive_buf: [u8; 8] = [0; 8];
        self.stream.read_exact(&mut receive_buf)?;
        Ok(parse_header(&receive_buf)?)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut read = 0;
        while read < buf.len() {
            let n = self
                .stream
                .read(&mut buf[read..])
                .context("connection closed")?;
            if n == 0 {
                anyhow::bail!("connection closed by peer");
            }
            read += n;
        }
        Ok(())
    }
}

fn calculate_value_size(data_type: u8, dimension: u16) -> usize {
    return if data_type == DataTypeId::BYTE as u8 {
        1
    } else if data_type == DataTypeId::SHORT as u8 {
        2
    } else if data_type == DataTypeId::INT as u8 || data_type == DataTypeId::FLOAT as u8 {
        4
    } else if data_type == DataTypeId::LONG as u8
        || data_type == DataTypeId::DOUBLE as u8
        || data_type == DataTypeId::TIMESTAMP as u8
    {
        8
    } else if data_type == DataTypeId::BYTE_VECTOR as u8 {
        1 * dimension as usize
    } else if data_type == DataTypeId::SHORT_VECTOR as u8 {
        2 * dimension as usize
    } else if data_type == DataTypeId::INT_VECTOR as u8
        || data_type == DataTypeId::FLOAT_VECTOR as u8
    {
        4 * dimension as usize
    } else if data_type == DataTypeId::LONG_VECTOR as u8
        || data_type == DataTypeId::FLOAT_VECTOR as u8
    {
        8 * dimension as usize
    } else {
        0
    };
}

pub fn value_to_string(value: &Vec<u8>, data_type: u8) -> Result<String> {
    let mut base_str = "".to_string();
    if (data_type < NUMERIC_ID_LIMIT) {
        if data_type == DataTypeId::LONG as u8 || data_type == DataTypeId::LONG as u8 {
            let arr: [u8; 8] = value.clone().try_into().unwrap();
            base_str.push_str(u64::from_le_bytes(arr).to_string().as_str())
        } else if data_type == DataTypeId::INT as u8 {
            let arr: [u8; 4] = value.clone().try_into().unwrap();
            base_str.push_str(u32::from_le_bytes(arr).to_string().as_str())
        } else if data_type == DataTypeId::SHORT as u8 {
            let arr: [u8; 2] = value.clone().try_into().unwrap();
            base_str.push_str(u16::from_le_bytes(arr).to_string().as_str())
        } else if (data_type == DataTypeId::BYTE as u8) {
            base_str.push_str(value[0].clone().to_string().as_str())
        } else {
        };
    } else if (data_type >= DataTypeId::BYTE_VECTOR as u8
        && data_type <= DataTypeId::DOUBLE_VECTOR as u8)
    {
        let size: usize = if data_type == DataTypeId::LONG_VECTOR as u8
            || data_type == DataTypeId::DOUBLE_VECTOR as u8
        {
            8
        } else if data_type == DataTypeId::INT_VECTOR as u8
            || data_type == DataTypeId::FLOAT_VECTOR as u8
        {
            4
        } else if data_type == DataTypeId::SHORT_VECTOR as u8 {
            2
        } else {
            1
        };

        for chunk in value.chunks_exact(size) {
            if (data_type == DataTypeId::LONG_VECTOR as u8) {
                let arr: [u8; 8] = chunk.try_into().unwrap();
                base_str.push_str(u64::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == DataTypeId::INT_VECTOR as u8 {
                let arr: [u8; 4] = chunk.try_into().unwrap();
                base_str.push_str(u32::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == DataTypeId::SHORT_VECTOR as u8 {
                let arr: [u8; 2] = chunk.try_into().unwrap();
                base_str.push_str(u16::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == DataTypeId::BYTE_VECTOR as u8 {
                base_str.push_str(chunk[0].to_string().as_str());
                base_str.push(',');
            } else if data_type == DataTypeId::FLOAT_VECTOR as u8 {
                let arr: [u8; 4] = chunk.try_into().unwrap();
                base_str.push_str(f32::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == DataTypeId::DOUBLE_VECTOR as u8 {
                let arr: [u8; 8] = chunk.try_into().unwrap();
                base_str.push_str(f64::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else {
                bail!("unsupported vector")
            }
        }
        base_str.pop();
    } else if (data_type == DataTypeId::BLOB as u8) {
        base_str.push_str(hex::encode(&value).as_str());
    } else if (data_type == DataTypeId::JSON as u8 || data_type == DataTypeId::VARCHAR as u8) {
        base_str.push_str(String::from_utf8(value.clone()).unwrap().as_str());
    } else {
        bail!("unsupported data type")
    }

    Ok(base_str)
}

#[cfg(test)]
mod tests {
    use crate::client::MDbClient;

    //RUST_BACKTRACE=full RUSTFLAGS="-Awarnings" cargo test test_client_connect -- --nocapture
    //#[test]
    fn test_client_connect() {
        let addr = "127.0.0.1:8081";
        let res = MDbClient::connect(addr);
        assert!(!res.is_ok());
    }

    //RUST_BACKTRACE=full RUSTFLAGS="-Awarnings" cargo test test_plain_query -- --nocapture
    //#[test]
    fn test_plain_query() {
        let addr = "127.0.0.1:8081";
        let mut res = MDbClient::connect(addr).unwrap();
        res.execute_plain_qeury(&"create table ks1.long_vectors (pk long,vec long_vector[8],uud json, primary key (pk))".to_string());
    }

    //RUST_BACKTRACE=full RUSTFLAGS="-Awarnings" cargo test test_plain_query_with_result -- --nocapture
    //#[test]
    fn test_plain_query_with_result() {
        let addr = "127.0.0.1:8081";
        let mut res = MDbClient::connect(addr).unwrap();
        let rows = res
            .execute_plain_qeury_with_result(
                &"select * from ks1.long_vectors where pk = 100".to_string(),
            )
            .unwrap();

        println!("rows {}", rows.get(0).unwrap());
    }

    //RUST_BACKTRACE=full RUSTFLAGS="-Awarnings" cargo test test_create_select_prep_query -- --nocapture
    //#[test]
    fn test_create_select_prep_query() {
        let addr = "127.0.0.1:8081";
        let mut client = MDbClient::connect(addr).unwrap();
        let mut prep = client
            .create_prepared(&"select * from ks1.long_vectors where pk = ?".to_string())
            .unwrap();

        prep.set_long(0, 800);

        let rows = client.execute_prepared(&prep).unwrap();
        println!("rows {}", rows.get(0).unwrap());
    }

    //RUST_BACKTRACE=full RUSTFLAGS="-Awarnings" cargo test test_create_insert_prep_query -- --nocapture
    //#[test]
    fn test_create_insert_prep_query() {
        let addr = "127.0.0.1:8081";
        let mut client = MDbClient::connect(addr).unwrap();
        let mut prep = client
            .create_prepared(
                &"insert into ks1.long_vectors (pk, vec, uud) values (?, ?, ?)".to_string(),
            )
            .unwrap();

        prep.set_long(0, 800);
        let vec: Vec<u64> = vec![10, 20, 30, 40, 50, 60, 43, 56];
        prep.set_long_vector(1, vec);
        prep.set_text(2, &"hello".to_string());

        let rows = client.execute_prepared(&prep).unwrap();
    }
}
