// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ptr,
};

use anyhow::{Ok, Result, bail, ensure};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref DATA_TYPES_MAP: HashMap<u8, DataType> = {
        let mut map = HashMap::new();
        map.insert(BYTE.id, BYTE);
        map.insert(SHORT.id, SHORT);
        map.insert(INT.id, INT);
        map.insert(LONG.id, LONG);
        map.insert(TIMESTAMP.id, TIMESTAMP);
        map.insert(BYTE_VECTOR.id, BYTE_VECTOR);
        map.insert(SHORT_VECTOR.id, SHORT_VECTOR);
        map.insert(INT_VECTOR.id, INT_VECTOR);
        map.insert(LONG_VECTOR.id, LONG_VECTOR);
        map.insert(FLOAT_VECTOR.id, FLOAT_VECTOR);
        map.insert(DOUBLE_VECTOR.id, DOUBLE_VECTOR);
        map.insert(JSON.id, JSON);
        map.insert(VARCHAR.id, VARCHAR);
        map.insert(BLOB.id, BLOB);
        map
    };
}

#[derive(Debug, Eq, Copy, Clone)]
#[repr(C)]
pub struct DataType {
    pub id: u8,
    pub bytes_size: u8,
    pub var_length: bool,
    pub vector: bool,
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.bytes_size.hash(state);
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

macro_rules! create_data_type {
    ($id:expr, $b:expr, $vl:expr, $v:expr) => {
        DataType {
            id: $id,
            bytes_size: $b,
            var_length: $vl,
            vector: $v,
        }
    };
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

pub static BYTE: DataType = DataType {
    id: DataTypeId::BYTE as u8,
    bytes_size: 1,
    var_length: false,
    vector: false,
};
pub static SHORT: DataType = create_data_type!(DataTypeId::SHORT as u8, 2, false, false);
pub static INT: DataType = create_data_type!(DataTypeId::INT as u8, 4, false, false);
pub static LONG: DataType = create_data_type!(DataTypeId::LONG as u8, 8, false, false);
pub static TIMESTAMP: DataType = create_data_type!(DataTypeId::TIMESTAMP as u8, 8, false, false);

pub static FLOAT: DataType = create_data_type!(DataTypeId::FLOAT as u8, 4, false, false);
pub static DOUBLE: DataType = create_data_type!(DataTypeId::DOUBLE as u8, 8, false, false);

pub static BYTE_VECTOR: DataType = create_data_type!(DataTypeId::BYTE_VECTOR as u8, 1, false, true);
pub static SHORT_VECTOR: DataType =
    create_data_type!(DataTypeId::SHORT_VECTOR as u8, 2, false, true);
pub static INT_VECTOR: DataType = create_data_type!(DataTypeId::INT_VECTOR as u8, 4, false, true);
pub static LONG_VECTOR: DataType = create_data_type!(DataTypeId::LONG_VECTOR as u8, 8, false, true);
pub static FLOAT_VECTOR: DataType =
    create_data_type!(DataTypeId::FLOAT_VECTOR as u8, 4, false, true);
pub static DOUBLE_VECTOR: DataType =
    create_data_type!(DataTypeId::DOUBLE_VECTOR as u8, 8, false, true);

pub static VARCHAR: DataType = create_data_type!(DataTypeId::VARCHAR as u8, 0, true, false);
pub static JSON: DataType = create_data_type!(DataTypeId::JSON as u8, 0, true, false);
pub static BLOB: DataType = create_data_type!(DataTypeId::BLOB as u8, 0, true, false);

pub static UNKNOWN: DataType = create_data_type!(DataTypeId::UNKNOWN as u8, 0, false, false);

pub static PK_ID_LIMIT: u8 = 6;
pub static NUMERIC_ID_LIMIT: u8 = 10;

pub fn map_data_type(t: u8) -> &'static DataType {
    DATA_TYPES_MAP.get(&t).expect("invalid data type")
}

pub fn parse_data_type(t: &String) -> &'static DataType {
    let d_t = t.to_lowercase();
    if (d_t.eq("json")) {
        return &JSON;
    } else if (d_t.eq("blob")) {
        return &BLOB;
    } else if (d_t.eq("varchar")) {
        return &VARCHAR;
    } else if (d_t.eq("double_vector")) {
        return &DOUBLE_VECTOR;
    } else if (d_t.eq("float_vector")) {
        return &FLOAT_VECTOR;
    } else if (d_t.eq("int_vector")) {
        return &INT_VECTOR;
    } else if (d_t.eq("long_vector")) {
        return &LONG_VECTOR;
    } else if (d_t.eq("short_vector")) {
        return &SHORT_VECTOR;
    } else if (d_t.eq("byte_vector")) {
        return &BYTE_VECTOR;
    } else if (d_t.eq("long")) {
        return &LONG;
    } else if (d_t.eq("int")) {
        return &INT;
    } else if (d_t.eq("short")) {
        return &SHORT;
    } else if (d_t.eq("byte")) {
        return &BYTE;
    } else if (d_t.eq("long")) {
        return &TIMESTAMP;
    } else if (d_t.eq("float")) {
        return &FLOAT;
    } else if (d_t.eq("double")) {
        return &DOUBLE;
    } else {
        return &UNKNOWN;
    }
}

pub fn convert_to_num(value: &Vec<u8>, d_type: u8) -> u64 {
    if d_type < NUMERIC_ID_LIMIT {
        if (d_type == LONG.id) {
            let ptr = value.as_ptr() as *const u64;
            u64::from_le(unsafe { ptr::read_unaligned(ptr) })
        } else if d_type == INT.id {
            let ptr = value.as_ptr() as *const u32;
            u32::from_le(unsafe { ptr::read_unaligned(ptr) }) as u64
        } else if d_type == SHORT.id {
            let ptr = value.as_ptr() as *const u16;
            u16::from_le(unsafe { ptr::read_unaligned(ptr) }) as u64
        } else if d_type == BYTE.id {
            value[0] as u64
        } else {
            0
        }
    } else {
        0
    }
}

pub fn parse_str_value(data_type: u8, str: &str, dimension: u16) -> Result<(u64, Vec<u8>)> {
    if (str.contains("?")) {
        return Ok((0, vec![]));
    }

    let dt = map_data_type(data_type);
    if data_type == LONG.id || data_type == TIMESTAMP.id {
        let parsed: u64 = str.parse::<u64>()?;
        Ok((parsed, u64::to_le_bytes(parsed).to_vec()))
    } else if data_type == INT.id {
        let parsed: u32 = str.parse::<u32>()?;
        Ok((parsed as u64, u32::to_le_bytes(parsed).to_vec()))
    } else if data_type == SHORT.id {
        let parsed: u16 = str.parse::<u16>()?;
        Ok((parsed as u64, u16::to_le_bytes(parsed).to_vec()))
    } else if data_type == BYTE.id {
        let parsed: u8 = str.parse::<u8>()?;
        Ok((parsed as u64, u8::to_le_bytes(parsed).to_vec()))
    } else if data_type == FLOAT.id {
        let parsed: f32 = str.parse::<f32>()?;
        Ok((parsed as u64, f32::to_le_bytes(parsed).to_vec()))
    } else if data_type == DOUBLE.id {
        let parsed: f64 = str.parse::<f64>()?;
        Ok((parsed as u64, f64::to_le_bytes(parsed).to_vec()))
    } else if dt.vector {
        let bytes = parse_vector(str, data_type, dimension)?;
        Ok((0, bytes))
    } else if data_type == JSON.id || data_type == BLOB.id || data_type == VARCHAR.id {
        Ok((0, str.as_bytes().to_vec()))
    } else {
        panic!("DB state incorrect")
    }
}

/*
   } else if data_type == DOUBLE_VECTOR.id {
       let bytes = str.as_bytes().to_vec();
       assert!(bytes.len() == dimension as usize * 8);
       for i in 0..dimension {
           let p = unsafe { bytes.as_ptr().add(i as usize * 8) as *const [u8;8] };
           let val = f64::from_le_bytes(unsafe { ptr::read_unaligned(p) });
       }
       (0, bytes)
*/
pub fn convert_prep_st_value(
    data: &[u8],
    data_type: u8,
    dim: u16,
    offset: u64,
) -> Result<(u32, Vec<u8>)> {
    //println!("parse prepared {} dim {} offset {}", data_type, dim, offset);
    let dt = map_data_type(data_type);

    if (data_type < NUMERIC_ID_LIMIT) {
        ensure!(
            data.len() - offset as usize >= dt.bytes_size as usize,
            "wrong data format"
        );

        let from = offset;
        let bytes_size = dt.bytes_size;
        let to = offset + bytes_size as u64;
        Ok((bytes_size as u32, data[from as usize..to as usize].to_vec()))
    } else if (dt.vector) {
        ensure!(
            data.len() - offset as usize >= dt.bytes_size as usize * dim as usize,
            "wrong data format"
        );

        let from = offset;
        let bytes_size = dt.bytes_size as u64 * dim as u64;
        let to = offset + bytes_size;
        Ok((bytes_size as u32, data[from as usize..to as usize].to_vec()))
    } else if (data_type == BLOB.id) {
        unsafe {
            ensure!(data.len() - offset as usize >= 4, "wrong data format");
            let p = unsafe { data.as_ptr().add(offset as usize) as *const u32 };
            let blob_len = u32::from_le(unsafe { ptr::read_unaligned(p) });
            if blob_len > 0 {
                let from = offset + 4;
                let to = offset + 4 + blob_len as u64;
                Ok((blob_len + 4, data[from as usize..to as usize].to_vec()))
            } else {
                Ok((4, vec![]))
            }
        }
    } else if (data_type == JSON.id || data_type == VARCHAR.id) {
        unsafe {
            ensure!(data.len() - offset as usize >= 2, "wrong data format");
            let p = unsafe { data.as_ptr().add(offset as usize) as *const u16 };
            let str_len = u16::from_le(unsafe { ptr::read_unaligned(p) });
            if str_len > 2 {
                let from = offset + 2;
                let to = offset + 2 + str_len as u64;
                Ok((
                    str_len as u32 + 2,
                    data[from as usize..to as usize].to_vec(),
                ))
            } else {
                Ok((2, vec![]))
            }
        }
    } else {
        bail!("unsupported data type")
    }
}

pub fn parse_vector(str: &str, data_type: u8, dimension: u16) -> Result<Vec<u8>> {
    let mut vector: Vec<u8> = vec![];

    let values: Vec<&str> = str.split(',').collect();

    assert_eq!(values.len(), dimension as usize);

    for s in values {
        if (data_type == LONG_VECTOR.id) {
            let bytes: Vec<u8> = s.trim().parse::<u64>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else if data_type == INT_VECTOR.id {
            let bytes: Vec<u8> = s.trim().parse::<u32>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else if data_type == SHORT_VECTOR.id {
            let bytes: Vec<u8> = s.trim().parse::<u16>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else if data_type == BYTE_VECTOR.id {
            let bytes: Vec<u8> = s.trim().parse::<u8>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else if data_type == FLOAT_VECTOR.id {
            let bytes: Vec<u8> = s.trim().parse::<f32>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else if data_type == DOUBLE_VECTOR.id {
            let bytes: Vec<u8> = s.trim().parse::<f64>()?.to_le_bytes().to_vec();
            vector.extend(bytes);
        } else {
            panic!("DB state incorrect");
        }
    }

    Ok(vector)
}

//method will panic in case of error cause it means DB state invalid or source code issue
pub fn value_to_string(value: &Vec<u8>, data_type: u8) -> String {
    let dt = map_data_type(data_type);
    let mut base_str = "".to_string();
    if (data_type < NUMERIC_ID_LIMIT) {
        if (data_type == LONG.id || data_type == TIMESTAMP.id) {
            let arr: [u8; 8] = value.clone().try_into().unwrap();
            base_str.push_str(u64::from_le_bytes(arr).to_string().as_str())
        } else if (data_type == INT.id) {
            let arr: [u8; 4] = value.clone().try_into().unwrap();
            base_str.push_str(u32::from_le_bytes(arr).to_string().as_str())
        } else if (data_type == SHORT.id) {
            let arr: [u8; 2] = value.clone().try_into().unwrap();
            base_str.push_str(u16::from_le_bytes(arr).to_string().as_str())
        } else if (data_type == BYTE.id) {
            base_str.push_str(value[0].clone().to_string().as_str())
        } else {
            panic!("unsupported data type")
        };
    } else if (dt.vector) {
        let size: usize = if data_type == LONG_VECTOR.id || data_type == DOUBLE_VECTOR.id {
            8
        } else if data_type == INT_VECTOR.id || data_type == FLOAT_VECTOR.id {
            4
        } else if data_type == SHORT_VECTOR.id {
            2
        } else {
            1
        };

        for chunk in value.chunks_exact(size) {
            if (data_type == LONG_VECTOR.id) {
                let arr: [u8; 8] = chunk.try_into().unwrap();
                base_str.push_str(u64::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == INT_VECTOR.id {
                let arr: [u8; 4] = chunk.try_into().unwrap();
                base_str.push_str(u32::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == SHORT_VECTOR.id {
                let arr: [u8; 2] = chunk.try_into().unwrap();
                base_str.push_str(u16::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == BYTE_VECTOR.id {
                base_str.push_str(chunk[0].to_string().as_str());
                base_str.push(',');
            } else if data_type == FLOAT_VECTOR.id {
                let arr: [u8; 4] = chunk.try_into().unwrap();
                base_str.push_str(f32::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else if data_type == DOUBLE_VECTOR.id {
                let arr: [u8; 8] = chunk.try_into().unwrap();
                base_str.push_str(f64::from_le_bytes(arr).to_string().as_str());
                base_str.push(',');
            } else {
                panic!("unsupported vector");
            }
        }
        base_str.pop();
    } else if (data_type == BLOB.id) {
        base_str.push_str(hex::encode(&value).as_str());
    } else if (data_type == JSON.id || data_type == VARCHAR.id) {
        base_str.push_str(String::from_utf8(value.clone()).unwrap().as_str());
    } else {
        panic!("unsupported data type")
    }

    base_str
}
