// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use crate::{
    data::{index::PrimaryKey, segment::ColumnDescription},
    model::data_type::{NUMERIC_ID_LIMIT, convert_to_num},
};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct Row {
    pub pk: PrimaryKey,
    pub timestamp: u64,
    pub tx_id: u64,
    pub payload: Vec<Column>,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub num_val: u64, //for numeric data types
    pub dimension: u16,
    pub d_type: u8,
    pub col_id: u8,
    pub flags: u8,
    pub placeholder: bool,
    pub value: Vec<u8>,
}

impl Column {
    pub fn new(col_id: u8, d_type: u8, dimension: u16, flags: u8, value: Vec<u8>) -> Self {
        let pk_num_val = convert_to_num(&value, d_type);

        Self {
            value,
            d_type,
            col_id,
            dimension,
            flags,
            num_val: pk_num_val,
            placeholder: false,
        }
    }

    pub fn placeholder(col_id: u8, data_type: u8, dimension: u16, flags: u8) -> Self {
        Self {
            dimension: dimension,
            d_type: data_type,
            col_id: col_id,
            flags: flags,
            value: vec![],
            num_val: 0,
            placeholder: true,
        }
    }

    /*
    pub fn get_bytes_value(&self) -> &Vec<u8> {
        &self.value
    }
    */

    pub fn get_size(&self) -> usize {
        self.value.len()
    }
}

#[macro_export]
macro_rules! new_column {
    ($col_id:expr, $d_type:expr, $dimension:expr, $flags:expr, $value:expr) => {
        Column::new($col_id, $d_type, $dimension, $flags, $value)
    };
}

#[macro_export]
macro_rules! placeholder {
    ($col_id:expr, $d_type:expr, $dimension:expr, $flags:expr) => {
        Column::placeholder($col_id, $d_type, $dimension, $flags)
    };
}

pub fn clone_column(d: &ColumnDescription, value: Vec<u8>) -> Column {
    new_column!(d.col_id, d.data_type.id, d.dimension, d.flags, value)
}

impl Hash for Column {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if (self.d_type < NUMERIC_ID_LIMIT) {
            self.num_val.hash(state);
        } else {
            self.value.hash(state);
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        let eq = if (self.d_type < NUMERIC_ID_LIMIT) {
            self.d_type == other.d_type && self.num_val == other.num_val
        } else {
            self.d_type == other.d_type && self.value == other.value
        };
        //println!("eq for {} {} - {}", eq, self.pk_num_val, other.pk_num_val);
        eq
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if (self.d_type < NUMERIC_ID_LIMIT) {
            Some(self.num_val.cmp(&other.num_val))
        } else {
            Some(self.value.cmp(&other.value))
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if (self.d_type < NUMERIC_ID_LIMIT) {
            self.num_val.cmp(&other.num_val)
        } else {
            self.value.cmp(&other.value)
        }
    }
}

impl Eq for Column {}
