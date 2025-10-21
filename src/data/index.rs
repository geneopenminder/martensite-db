// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::ptr;
use std::sync::Arc;

use crate::data::MmapStruct;
use crate::data::segment::{
    ColumnDescription, ENTITY_HEADER_LENGTH, EntityHeader, FromToMmap, PkRange, check_crc, put_crc,
};
use crate::model::Column;
use crate::model::column::Row;
use crate::model::data_type::{DataType, NUMERIC_ID_LIMIT, convert_to_num};
use crate::protocol::{PK_FLAG, QueryConditionType};

use anyhow::{Result, bail, ensure};

/// Unique & range indexes for primary key
/// All indexes mmaps locked in memory with mlock()

const MISS_VALUE: u64 = u64::MAX;
const EXTRA_KEYS: u32 = 16;
const VAL_OFFSET: u32 = 8;
const ENTRY_SIZE: u32 = 16;
const EXTRA_SIZE: u32 = 64;
const DEFAULT_FREE_KEY: u64 = 0;

pub fn pk_keys_bitmap(columns: &Vec<ColumnDescription>) -> u64 {
    let mut keys_bitmap: u64 = 0;
    for c in columns {
        if c.flags & PK_FLAG > 0 {
            keys_bitmap |= 0x1 << c.col_id;
        }
    }
    keys_bitmap
}

#[derive(Debug, Clone)]
pub struct NumericPrimaryKey {
    pub key_vector: Vec<u64>,
}

impl Hash for NumericPrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key_vector.hash(state);
        for c in &self.key_vector {
            c.hash(state);
        }
    }
}

impl PartialEq for NumericPrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        self.key_vector == other.key_vector
    }
}

impl PartialOrd for NumericPrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Option::Some(self.key_vector.cmp(&other.key_vector))
    }
}

impl Ord for NumericPrimaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key_vector.cmp(&other.key_vector)
    }
}

impl Eq for NumericPrimaryKey {}

pub trait PkIdx: Send + Sync {
    fn get(&self, pk: &PrimaryKey) -> u64;
    fn put(&mut self, pk: &PrimaryKey, val: u64);
    fn flush(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn path(&self) -> String;
    fn all_values(&self) -> Vec<u64>;
}

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub primary: Column,
    pub secondary: Option<Vec<Column>>,
}

impl Hash for PrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.primary.hash(state);
        if (self.secondary.is_some()) {
            for c in self.secondary.as_ref().unwrap() {
                c.hash(state);
            }
        }
    }
}

impl PartialEq for PrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        let eq = self.primary == other.primary;
        if (eq) {
            return match (&self.secondary, &other.secondary) {
                (Some(s), Some(o)) => s == o,
                _ => eq,
            };
        }
        return eq;
    }
}

impl PartialOrd for PrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let ordering = self.primary.cmp(&other.primary);
        match ordering {
            std::cmp::Ordering::Equal => match (&self.secondary, &other.secondary) {
                (Some(s), Some(o)) => Option::Some(s.cmp(o)),
                _ => Option::Some(ordering),
            },
            _ => Option::Some(ordering),
        }
    }
}

impl Ord for PrimaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ordering = self.primary.cmp(&other.primary);
        match ordering {
            std::cmp::Ordering::Equal => match (&self.secondary, &other.secondary) {
                (Some(s), Some(o)) => s.cmp(o),
                _ => ordering,
            },
            _ => ordering,
        }
    }
}

impl Eq for PrimaryKey {}

/// Open adressing hash map for mmap persistence u64-u64
///
/// ----
/// offset      | k-v
/// 0x01-0x07   | key1 u64    
/// 0x08-0x0E   | val1 u64
/// 0xXX-0xXX   | 0 - empty entry u64    
/// 0xXX+8-0xXX | 0   u64
/// ......................
/// 0xXX-0xXX   | keyN u64    
/// 0xXX+8-0xXX | valN u64    
///
/// last 64 bytes header
///
/// offset      | k-v
/// 0x00-0x07   | zero key value u64    
/// 0x08-0x0B   | keys count u32 -- occupied entries
/// 0x0C        | has zero key u8 -- bool
/// 0x0F-0x17   | crc u32 -- calculated for values up to header offset

pub struct U64OAHashMapIdx {
    pub path: String,
    pub map_size: u32,
    pub file: MmapStruct,
    has_zero_key: bool,
    zero_val: u64,
    pub keys_count: u32,
}

//TODO
pub fn long_hash(key: u64) -> u32 {
    //let h = key.hash(&mut self.hasher);
    //self.hasher.finish() as u32
    let k = key as i64;
    let h = k.wrapping_mul(-7046029254386353131) as i64;
    return (h ^ h >> 32) as u32;
}

impl U64OAHashMapIdx {
    pub fn new(path: &String, elements_count: u32) -> Result<Self> {
        let leading_zeros = elements_count.leading_zeros();
        let mut map_size = u32::pow(2, 32 - leading_zeros);

        if (map_size as f64 * 0.75 < elements_count as f64) {
            map_size <<= 0x1;
        }

        //println!("map size new {} elems count {}", map_size, elements_count);
        let file_size = (map_size + EXTRA_KEYS) * ENTRY_SIZE + EXTRA_SIZE;
        let mut file = MmapStruct::new_limited(path, file_size as u64)?;
        //fill zero bytes
        let ptr = file.mmap.as_mut_ptr();
        unsafe {
            ptr::write_bytes(
                ptr,
                0,
                (map_size + EXTRA_KEYS) as usize * ENTRY_SIZE as usize,
            )
        };

        Ok(U64OAHashMapIdx {
            path: path.clone(),
            map_size,
            file,
            has_zero_key: false,
            zero_val: 0,
            keys_count: 0,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        let meta_offset = self.file.file_size - EXTRA_SIZE as u64;
        //println!("meta offset {}", meta_offset);
        self.file
            .put_u64(meta_offset as usize + 0, self.zero_val as u64);
        self.file.put_u32(meta_offset as usize + 8, self.keys_count);
        self.file.put_u32(
            meta_offset as usize + 12,
            if (self.has_zero_key) { 1 } else { 0 },
        );
        let crc = self.file.calculate_crc_limited(meta_offset as usize);
        self.file.put_u32(meta_offset as usize + 16, crc);
        self.file.flush()?;
        self.file.close()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let meta_offset = self.file.file_size - EXTRA_SIZE as u64;
        //println!("meta offset {}", meta_offset);
        self.file
            .put_u64(meta_offset as usize + 0, self.zero_val as u64);
        self.file.put_u32(meta_offset as usize + 8, self.keys_count);
        self.file.put_u32(
            meta_offset as usize + 12,
            if (self.has_zero_key) { 1 } else { 0 },
        );
        let crc = self.file.calculate_crc_limited(meta_offset as usize);
        self.file.put_u32(meta_offset as usize + 16, crc);
        self.file.flush()?;
        self.file.close()?;
        Ok(())
    }

    pub fn load(path: &String) -> Result<Self> {
        let file = MmapStruct::load_mlock(&path)?;
        let elements_count =
            ((file.file_size - EXTRA_SIZE as u64) / ENTRY_SIZE as u64) - EXTRA_KEYS as u64;
        //println!("map size load {} elems count {}", mapped_file.mapped_file.len(), elements_count);
        let meta_offset = file.file_size - EXTRA_SIZE as u64;

        let crc = file.get_u32(meta_offset as usize + 16);
        let crc_calculated = file.calculate_crc_limited(meta_offset as usize);
        ensure!(
            crc == crc_calculated,
            "crc invalid for {}; remove all segment files and restart DB",
            file.path
        );

        let zero_val = file.get_u64(meta_offset as usize + 0);
        let keys_count = file.get_u32(meta_offset as usize + 8);
        let has_zero_key = if (file.get_u32(meta_offset as usize + 12)) > 0 {
            true
        } else {
            false
        };

        Ok(U64OAHashMapIdx {
            path: path.clone(),
            map_size: elements_count as u32,
            file,
            has_zero_key: has_zero_key,
            zero_val: zero_val as u64,
            keys_count: keys_count,
        })
    }

    pub fn put(&mut self, key: u64, val: u64) -> u64 {
        let mut ret = val;
        let mut key_exists = false;

        if (key == 0) {
            if (self.has_zero_key) {
                ret = self.zero_val;
            } else {
                self.has_zero_key = true;
                self.keys_count += 1;
            }
            self.zero_val = val;
            return ret;
        }

        let hash_code = long_hash(key);
        let key_index: u32 = hash_code & ((self.map_size) - 1);
        let mut added = false;

        let mut key_addr = 0;
        let mut value_addr = 0;

        let mut last_idx = 0;
        for index in key_index..self.map_size + EXTRA_KEYS {
            last_idx = index;
            key_addr = last_idx * ENTRY_SIZE;
            value_addr = key_addr + VAL_OFFSET;
            let existingKey: u64 = self.file.get_u64(key_addr as usize) as u64;
            if (existingKey == DEFAULT_FREE_KEY as u64) {
                //println!("idx {}", index);
                added = true;
                self.file.put_u64(key_addr as usize, key as u64);
                self.file.put_u64(value_addr as usize, val as u64);
                break;
            } else if (existingKey == key) {
                key_exists = true;
                ret = self.file.get_u64(value_addr as usize) as u64;
                self.file.put_u64(key_addr as usize, key as u64);
                self.file.put_u64(value_addr as usize, val as u64);
                added = true;
                break;
            }
        }

        if (!added) {
            panic!("no space in map");
        }

        if (!key_exists) {
            self.keys_count += 1;
        }

        ret
    }

    pub fn get(&self, key: u64) -> u64 {
        if (key == 0 && self.has_zero_key) {
            return self.zero_val;
        }

        let mut ret = MISS_VALUE;
        let hash_code = long_hash(key);
        let key_index: u32 = hash_code & ((self.map_size) - 1);

        for index in key_index..self.map_size + EXTRA_KEYS {
            //println!("idx {}", index);
            let key_addr = index * ENTRY_SIZE;
            let value_addr = key_addr + VAL_OFFSET;
            let existing_key = self.file.get_u64(key_addr as usize) as u64;
            if (existing_key == DEFAULT_FREE_KEY) {
                return MISS_VALUE;
            } else if (existing_key == key) {
                return self.file.get_u64(value_addr as usize) as u64;
            }
        }
        ret
    }

    pub fn get_keys(&self) -> Vec<u64> {
        let mut keys: Vec<u64> = Vec::with_capacity(self.keys_count as usize);
        let mut idx = 0;
        for index in idx..self.map_size + EXTRA_KEYS {
            let key_addr = index * ENTRY_SIZE;
            let existing_key = self.file.get_u64(key_addr as usize) as u64;
            if (existing_key != DEFAULT_FREE_KEY) {
                keys.push(existing_key);
            }
        }

        keys
    }

    pub fn get_values(&self) -> Vec<u64> {
        let mut values: Vec<u64> = Vec::with_capacity(self.keys_count as usize);
        let mut idx = 0;
        for index in idx..self.map_size + EXTRA_KEYS {
            let key_addr = index * ENTRY_SIZE;
            let existing_key = self.file.get_u64(key_addr as usize) as u64;
            if (existing_key != DEFAULT_FREE_KEY) {
                let value_addr = key_addr + VAL_OFFSET;
                let value = self.file.get_u64(value_addr as usize) as u64;
                values.push(value);
            }
        }

        values
    }

    pub fn size(&self) -> u32 {
        self.keys_count
    }
}

impl PkIdx for U64OAHashMapIdx {
    fn get(&self, pk: &PrimaryKey) -> u64 {
        self.get(pk.primary.num_val)
    }

    fn put(&mut self, pk: &PrimaryKey, val: u64) {
        self.put(pk.primary.num_val, val);
    }

    fn flush(&mut self) -> Result<()> {
        self.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.close()?;
        Ok(())
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn all_values(&self) -> Vec<u64> {
        self.get_values()
    }
}

pub struct CompositePkOAHashMapIdx {
    pub path: String,
    pub map_size: u32,
    file: MmapStruct,
    zero_val: u64,
    pub keys_bitmap: u64,
    pub keys_count: u32,
    pub entry_size: u32,
    free_key: Vec<u8>,
    has_zero_key: bool,
    pub pk_size: u8,
}

//TODO fast hash algo
pub fn composite_hash(key: &PrimaryKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Open adressing hash map for mmap persistence composite numeric key [1,3,...] - val u64
/// key stored as bytes array of LITTLE_ENDIAN values with 8 bytes alignment
/// example key (2 u64, 45 u32, 66 u64) -- [u8] --
/// [2 as u64 LE 8 bytes + 45 as u32 LE 4 bytes + 66 as u64 LE 8 bytes ] -- 20 bytes  -- key size with alignment 24 bytes + 8 bytes value -> entry length 32 bytes
/// ----
/// offset      | k-v
/// 0x01-0x0N   | key1 [u8]    
/// 0x0N-0xXX   | val1 u64
/// ......................
/// 0xXX-0xXN   | keyN u64    
/// 0xXX-0xXX   | valN u64    
///
/// last 64 bytes header
///
/// offset      | k-v
/// 0x00-0x07   | zero key value u64
/// 0x08-0x0E   | keys bitmap u64 -- bits for column ids of primary key -- max columns count is 64
/// 0x0F-0x13   | keys count u32 -- occupied entries
/// 0x14        | has zero key u8 -- bool
/// 0x15        | pk size in bytes -- sum of all pk columns byte sizes
/// 0x16        | entry size in bytes
/// 0x17-0x1A   | crc u32 -- calculated for values up to header offset
///

impl CompositePkOAHashMapIdx {
    pub fn new(
        path: &String,
        elements_count: u32,
        pk_columns: &Vec<ColumnDescription>,
    ) -> Result<Self> {
        let leading_zeros = elements_count.leading_zeros();
        let mut map_size = u32::pow(2, 32 - leading_zeros);

        if (map_size as f64 * 0.75 < elements_count as f64) {
            map_size <<= 0x1;
        }

        println!(
            "composite map size new {} elems count {}",
            map_size, elements_count
        );

        let keys_bitmap: u64 = pk_keys_bitmap(pk_columns);

        let pk_size = pk_columns.iter().map(|c| c.data_type.bytes_size).sum();
        let entry_size = (pk_size / 8) * 8 + if pk_size % 8 == 0 { 0 } else { 8 } + 8;
        println!("composite pk size {} entry size {}", pk_size, entry_size);

        let file_size = (map_size + EXTRA_KEYS) * entry_size as u32 + EXTRA_SIZE;

        let mut mapped_file = MmapStruct::new_limited(&path, file_size as u64)?;

        for i in 0..map_size + EXTRA_KEYS {
            let offset = i as usize * entry_size as usize;
            for p_i in 0..pk_size {
                mapped_file.put_u8(offset + p_i as usize, 0);
            }
        }

        let free_key: Vec<u8> = vec![0; pk_size as usize];
        Ok(CompositePkOAHashMapIdx {
            path: path.clone(),
            map_size: map_size,
            file: mapped_file,
            has_zero_key: false,
            zero_val: 0,
            keys_count: 0,
            pk_size: pk_size,
            entry_size: entry_size as u32,
            free_key: free_key,
            keys_bitmap: keys_bitmap,
        })
    }

    pub fn load(path: &String) -> Result<Self> {
        //let mut hasher = DefaultHasher::new();
        let mut file = MmapStruct::load_mlock(&path)?;
        //println!("map size load {} elems count {}", mapped_file.mapped_file.len(), elements_count);
        let meta_offset = file.file_size as usize - EXTRA_SIZE as usize;

        let crc = file.get_u32(meta_offset as usize + 24);
        let crc_calculated = file.calculate_crc_limited(meta_offset as usize);
        ensure!(
            crc == crc_calculated,
            "crc invalid for {}; remove all segment files and restart DB",
            file.path
        );

        let zero_val = file.get_u64(meta_offset + 0);
        let keys_bitmap = file.get_u64(meta_offset + 8);
        let keys_count = file.get_u32(meta_offset + 16);
        let has_zero_key = if (file.get_u32(meta_offset + 20)) > 0 {
            true
        } else {
            false
        };
        let pk_size = file.get_u8(meta_offset + 21);
        let entry_size = file.get_u8(meta_offset + 22);
        let free_key: Vec<u8> = vec![0; pk_size as usize];
        let elements_count =
            ((file.file_size - EXTRA_SIZE as u64) / entry_size as u64) - EXTRA_KEYS as u64;

        Ok(CompositePkOAHashMapIdx {
            path: path.clone(),
            map_size: elements_count as u32,
            file,
            has_zero_key: has_zero_key,
            zero_val: zero_val as u64,
            keys_count: keys_count,
            pk_size: pk_size,
            entry_size: entry_size as u32,
            free_key: free_key,
            keys_bitmap: keys_bitmap,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        let meta_offset = self.file.file_size - EXTRA_SIZE as u64;
        self.file
            .put_u64(meta_offset as usize + 0, self.zero_val as u64);
        self.file
            .put_u64(meta_offset as usize + 8, self.keys_bitmap);
        self.file
            .put_u32(meta_offset as usize + 16, self.keys_count);
        self.file.put_u8(
            meta_offset as usize + 20,
            if (self.has_zero_key) { 1 } else { 0 },
        );
        self.file.put_u8(meta_offset as usize + 21, self.pk_size);
        self.file
            .put_u8(meta_offset as usize + 22, self.entry_size as u8);
        let crc = self.file.calculate_crc_limited(meta_offset as usize);
        self.file.put_u32(meta_offset as usize + 24, crc);
        self.file.flush()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let base_offset = self.file.file_size - EXTRA_SIZE as u64;
        let meta_offset = self.file.file_size - EXTRA_SIZE as u64;
        self.file
            .put_u64(meta_offset as usize + 0, self.zero_val as u64);
        self.file
            .put_u64(meta_offset as usize + 8, self.keys_bitmap);
        self.file
            .put_u32(meta_offset as usize + 16, self.keys_count);
        self.file.put_u8(
            meta_offset as usize + 20,
            if (self.has_zero_key) { 1 } else { 0 },
        );
        self.file.put_u8(meta_offset as usize + 21, self.pk_size);
        self.file
            .put_u8(meta_offset as usize + 22, self.entry_size as u8);
        let crc = self.file.calculate_crc_limited(meta_offset as usize);
        self.file.put_u32(meta_offset as usize + 24, crc);
        self.file.close()?;
        Ok(())
    }

    pub fn get_pk_bytes(&self, key: &PrimaryKey) -> (Vec<u8>, bool) {
        let pk = if key.primary.d_type < NUMERIC_ID_LIMIT {
            key.primary.num_val
        } else {
            convert_to_num(&key.primary.value, key.primary.d_type)
        };
        let mut key_values: Vec<u8> = vec![0; self.pk_size as usize];
        let primary_len = &key.primary.value.len();
        key_values[0..*primary_len].copy_from_slice(&key.primary.value);
        let mut null_pk: bool = pk == 0;

        let mut offset: usize = *primary_len;
        match key.secondary.as_ref() {
            Some(sec) => {
                for s in sec {
                    let sec_k = if key.primary.d_type < NUMERIC_ID_LIMIT {
                        s.num_val
                    } else {
                        convert_to_num(&s.value, s.d_type)
                    };
                    if (sec_k > 0) {
                        null_pk = false;
                    }
                    let sec_len = s.value.len();
                    key_values[offset..offset + sec_len].copy_from_slice(&s.value);
                    offset += sec_len;
                }
            }
            None => (),
        }
        (key_values, null_pk)
    }

    pub fn put(&mut self, key: &PrimaryKey, val: u64) -> u64 {
        let mut ret = val;
        let mut key_exists = false;

        let (pk_bytes, null_pk) = self.get_pk_bytes(key);

        if (null_pk) {
            if (self.has_zero_key) {
                ret = self.zero_val;
            } else {
                self.has_zero_key = true;
                self.keys_count += 1;
            }
            self.zero_val = val;
            return ret;
        }

        let hash_code = composite_hash(&key);
        let key_index: u32 = hash_code as u32 & (self.map_size - 1); //TODO
        let mut added = false;

        //println!("put key idx {} map size {}", key_index, self.map_size);

        let val_offset = self.entry_size - 8;

        for index in key_index..self.map_size + EXTRA_KEYS {
            let key_addr = index * self.entry_size;
            let value_addr = key_addr + val_offset;

            let is_free = compare_by_bytes(
                self.free_key.as_ptr(),
                unsafe { self.file.mmap.as_ptr().add(key_addr as usize) },
                self.free_key.len(),
            );

            if (is_free) {
                added = true;
                self.file.put_bytes(key_addr as usize, &pk_bytes);
                self.file.put_u64(value_addr as usize, val as u64);
                break;
            } else if (compare_by_bytes(
                pk_bytes.as_ptr(),
                unsafe { self.file.mmap.as_ptr().add(key_addr as usize) },
                self.free_key.len(),
            )) {
                key_exists = true;
                ret = self.file.get_u64(value_addr as usize) as u64;
                self.file.put_bytes(key_addr as usize, &pk_bytes);
                self.file.put_u64(value_addr as usize, val as u64);
                added = true;
                break;
            }
        }

        if (!added) {
            panic!("no space in hashmap");
        }

        if (!key_exists) {
            self.keys_count += 1;
        }

        ret
    }

    pub fn get(&self, key: &PrimaryKey) -> u64 {
        let (pk_bytes, null_pk) = self.get_pk_bytes(&key);

        if (null_pk && self.has_zero_key) {
            return self.zero_val;
        }

        let mut ret = MISS_VALUE;
        let hash_code = composite_hash(&key);
        let key_index: u32 = hash_code as u32 & (self.map_size - 1); //TODO

        let val_offset = self.entry_size - 8;

        for index in key_index..self.map_size + EXTRA_KEYS {
            let key_addr = index * self.entry_size;
            let value_addr = key_addr + val_offset;

            let is_free = compare_by_bytes(
                self.free_key.as_ptr(),
                unsafe { self.file.mmap.as_ptr().add(key_addr as usize) },
                self.free_key.len(),
            );
            if (is_free) {
                return MISS_VALUE;
            } else if (compare_by_bytes(
                pk_bytes.as_ptr(),
                unsafe { self.file.mmap.as_ptr().add(key_addr as usize) },
                self.free_key.len(),
            )) {
                return self.file.get_u64(value_addr as usize) as u64;
            }
        }
        ret
    }

    pub fn get_values(&self) -> Vec<u64> {
        let mut values: Vec<u64> = Vec::with_capacity(self.keys_count as usize);

        let val_offset = self.entry_size - 8;
        for index in 0..self.map_size + EXTRA_KEYS {
            let key_addr = index * self.entry_size;
            let value_addr = key_addr + val_offset;

            let is_free = compare_by_bytes(
                self.free_key.as_ptr(),
                unsafe { self.file.mmap.as_ptr().add(key_addr as usize) },
                self.free_key.len(),
            );
            if (!is_free) {
                let value = self.file.get_u64(value_addr as usize) as u64;
                values.push(value);
            }
        }

        values
    }
}

impl PkIdx for CompositePkOAHashMapIdx {
    fn get(&self, pk: &PrimaryKey) -> u64 {
        self.get(pk)
    }

    fn put(&mut self, pk: &PrimaryKey, val: u64) {
        self.put(pk, val);
    }

    fn flush(&mut self) -> Result<()> {
        self.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.close()?;
        Ok(())
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn all_values(&self) -> Vec<u64> {
        self.get_values()
    }
}

fn compare_by_bytes(p1: *const u8, p2: *const u8, size: usize) -> bool {
    let p1_u8 = p1 as *const u8;
    let p2_u8 = p2 as *const u8;

    for i in 0..size {
        if unsafe { *p1_u8.add(i) } != unsafe { *p2_u8.add(i) } {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone)]
pub struct RangeIndexKey {
    pub primary: u64,
    pub from: u64,
    pub to: u64,
    pub secondary_keys: Option<BTreeMap<u64, RangeIndexKey>>,
    pub value: Option<Arc<Row>>,
}

impl Hash for RangeIndexKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.primary.hash(state);
        //self.key.hash(state);
    }
}

impl PartialEq for RangeIndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.primary == other.primary
    }
}

impl Eq for RangeIndexKey {}

impl PartialOrd for RangeIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.primary.partial_cmp(&other.primary)
    }
}

impl Ord for RangeIndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.primary.cmp(&other.primary)
    }
}

#[derive(Debug, Clone)]
pub struct NestedIndexKey {
    key: u64,
    keys: BTreeSet<RangeIndexKey>,
}

impl Ord for NestedIndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for NestedIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl PartialEq for NestedIndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for NestedIndexKey {}

pub fn get_pk_values(key: &PrimaryKey) -> (Vec<u8>, Vec<u64>) {
    let mut pk_nums: Vec<u64> = vec![];
    let mut pk_bytes: Vec<u8> = vec![];

    let primary_num = convert_to_num(&key.primary.value, key.primary.d_type);
    pk_nums.push(primary_num);
    pk_bytes.extend(&key.primary.value);

    if key.secondary.is_some() {
        for s in key.secondary.as_ref().unwrap() {
            let sec_num = convert_to_num(&s.value, s.d_type);
            pk_nums.push(sec_num);
            pk_bytes.extend(&s.value);
        }
    }
    (pk_bytes, pk_nums)
}

pub fn get_pk_num_values(pk_bytes: &Vec<u8>, columns: &HashMap<u8, ColumnDescription>) -> Vec<u64> {
    let mut result: Vec<u64> = vec![];

    let mut offset: usize = 0;

    for i in 0..columns.len() {
        let d = columns.get(&(i as u8)).unwrap();
        if d.flags & PK_FLAG > 0 {
            let byte_size = d.data_type.bytes_size;
            let val = convert_to_num(
                &pk_bytes[offset..offset + byte_size as usize].to_vec(),
                d.data_type.id,
            );
            offset += byte_size as usize;
            result.push(val);
        }
    }

    result
}

/// Range index for memtable as endless depth of nested TreeMaps
pub struct RangeMemIdx {
    pub idx_id: u32,
    pub keys_bitmap: u64,
    pub idx_name: String,
    pub columns: Vec<ColumnDescription>,
    pub keys: BTreeMap<u64, RangeIndexKey>,
}

pub fn range_idx_insert(
    idx: &mut BTreeMap<u64, RangeIndexKey>,
    idx_key: &Vec<&Column>,
    row: &Arc<Row>,
    depth: usize,
) {
    let first_key = idx_key.get(depth).unwrap();
    let mut key = RangeIndexKey {
        primary: first_key.num_val,
        from: 0,
        to: 0,
        secondary_keys: Option::None,
        value: Option::Some(Arc::clone(row)),
    };

    let mut exist = idx.get_mut(&first_key.num_val);

    if depth == idx_key.len() - 1 {
        match exist {
            Some(ref mut k) => {
                k.value = Option::Some(Arc::clone(row));
            }
            None => {
                idx.insert(key.primary, key);
            }
        }
    } else {
        match exist {
            Some(ref mut k) => {
                range_idx_insert(k.secondary_keys.as_mut().unwrap(), idx_key, row, depth + 1);
            }
            None => {
                let mut sec: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
                range_idx_insert(&mut sec, idx_key, row, depth + 1);
                key.secondary_keys = Option::Some(sec);
                idx.insert(key.primary, key);
            }
        }
    }
}

pub fn range_idx_select_eq(
    idx: &BTreeMap<u64, RangeIndexKey>,
    idx_key: &Vec<&Column>,
    depth: usize,
) -> Option<Arc<Row>> {
    let first_key = idx_key.get(depth).unwrap();
    let exist = idx.get(&first_key.num_val);

    if depth == idx_key.len() - 1 {
        match exist {
            Some(k) => {
                return Option::Some(Arc::clone(k.value.as_ref().unwrap()));
            }
            None => {
                return Option::None;
            }
        }
    } else {
        match exist {
            Some(k) => {
                return range_idx_select_eq(k.secondary_keys.as_ref().unwrap(), idx_key, depth + 1);
            }
            None => {
                return Option::None;
            }
        }
    }
}

pub fn range_idx_select_conditions(
    idx: &BTreeMap<u64, RangeIndexKey>,
    idx_key: &Vec<u64>,
    conditions: &Vec<QueryConditionType>,
    depth: usize,
) -> Option<Vec<Arc<Row>>> {
    let first_key = idx_key.get(depth).unwrap();
    let c = conditions.get(depth).unwrap();

    if depth == idx_key.len() - 1 {
        if matches!(c, QueryConditionType::EQ) {
            let exist = idx.get(&first_key);
            match exist {
                Some(k) => {
                    let val = Arc::clone(k.value.as_ref().unwrap());
                    return Option::Some(vec![val]);
                }
                None => {
                    return Option::None;
                }
            }
        } else {
            let range = match c {
                QueryConditionType::GT => idx.range(first_key + 1..),
                QueryConditionType::LT => idx.range(..first_key),
                QueryConditionType::GTE => idx.range(first_key..),
                QueryConditionType::LTE => idx.range(..=first_key),
                _ => todo!(),
            };

            let mut rows: Vec<Arc<Row>> = vec![];

            for (k, v) in range {
                rows.push(Arc::clone(v.value.as_ref().unwrap()));
            }

            if rows.is_empty() {
                return Option::None;
            } else {
                return Option::Some(rows);
            }
        }
    } else {
        if matches!(c, QueryConditionType::EQ) {
            let exist = idx.get(&first_key);
            match exist {
                Some(k) => {
                    return range_idx_select_conditions(
                        k.secondary_keys.as_ref().unwrap(),
                        idx_key,
                        conditions,
                        depth + 1,
                    );
                }
                None => {
                    return Option::None;
                }
            }
        } else {
            let range = match c {
                QueryConditionType::GT => idx.range(first_key + 1..),
                QueryConditionType::LT => idx.range(..first_key - 1),
                QueryConditionType::GTE => idx.range(first_key..),
                QueryConditionType::LTE => idx.range(..=first_key),
                _ => todo!(),
            };

            let mut rows: Vec<Arc<Row>> = vec![];

            for (k, v) in range {
                let res = range_idx_select_conditions(
                    v.secondary_keys.as_ref().unwrap(),
                    idx_key,
                    conditions,
                    depth + 1,
                );
                match res {
                    Some(resp) => {
                        for r in resp.iter() {
                            rows.push(Arc::clone(r));
                        }
                    }
                    None => {}
                }
            }
            return Option::Some(rows);
        }
    }

    Option::None
}

#[derive(Debug, Clone)]
pub enum IdxType {
    UNIQUE = 1,
    VECTOR = 2,
    Range = 3,
}

pub fn idx_type_to_str(idx_type: &IdxType) -> String {
    match idx_type {
        IdxType::Range => "range".to_string(),
        IdxType::UNIQUE => "uniq".to_string(),
        IdxType::VECTOR => "vector".to_string(),
    }
}

pub fn map_idx_type(idx_type: u8) -> IdxType {
    match idx_type {
        1 => IdxType::UNIQUE,
        2 => IdxType::VECTOR,
        3 => IdxType::Range,
        _ => todo!(),
    }
}

pub fn get_idx_id(idx_type: &IdxType) -> u8 {
    match idx_type {
        IdxType::UNIQUE => 1,
        IdxType::VECTOR => 2,
        IdxType::Range => 3,
    }
}
pub struct IdxMetaStruct {
    pub header: EntityHeader,
    pub keys_bitmap: u64,
    pub idx_name: String,
    pub idx_type: IdxType,
    pub columns: Vec<u8>,
}

pub static IDX_META_OFFSET: usize = 64;

impl IdxMetaStruct {
    pub fn new(
        meta_path: &String,
        idx_name: &String,
        header: &EntityHeader,
        idx_type: IdxType,
        columns: &Vec<u8>,
    ) -> Result<Self> {
        let mut file = MmapStruct::new_limited(&meta_path, 4096)?;
        header.save_to_mapped_file(&mut file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        file.put_string_with_size(offset, &idx_name.as_bytes());

        let mut offset = IDX_META_OFFSET;
        let mut keys_bitmap: u64 = 0;
        for c in columns {
            keys_bitmap |= 0x1 << c;
        }

        file.put_u64(offset, keys_bitmap);
        offset += 8;
        file.put_u8(offset, get_idx_id(&idx_type));
        offset += 1;
        file.put_u8(offset, columns.len() as u8);
        offset += 1;

        file.put_bytes(offset, columns);

        put_crc(&mut file);
        file.flush()?;

        Ok(IdxMetaStruct {
            header: header.clone(),
            keys_bitmap,
            idx_type,
            columns: columns.clone(),
            idx_name: idx_name.to_string(),
        })
    }

    pub fn load(path: &String) -> Result<Self> {
        let file = MmapStruct::load(path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        let idx_name = file.get_sized_string(offset).1.to_vec();

        let mut offset = IDX_META_OFFSET;

        let keys_bitmap = file.get_u64(offset);
        offset += 8;
        let idx_type = map_idx_type(file.get_u8(offset));
        offset += 1;
        let col_count = file.get_u8(offset);
        offset += 1;

        let columns: Vec<u8> = file.get_bytes(offset, col_count as usize);
        println!("idx columns {:?}", columns);

        Ok(IdxMetaStruct {
            header: header,
            idx_type,
            columns: columns.clone(),
            idx_name: String::from_utf8(idx_name)?,
            keys_bitmap,
        })
    }
}

static RANGE_PK_OFFSET: usize = 64; // 8 * 128
pub static RANGE_IDX_DATA_OFFSET: usize = 1024; // 8 * 128

pub struct RangeIdxStruct {
    pub mapped_file: MmapStruct,
    pub header: EntityHeader,
    pub keys_bitmap: u64,
    pub segment_id: u32,
    pub columns: Vec<u8>,
    pub range: PkRange,
}

pub struct IdxSegmentRange {
    pub pk_num_from: u64,
    pub pk_num_to: u64,
    pub pk_bytes_from: Vec<u8>,
    pub pk_bytes_to: Vec<u8>,
    pub single_num: bool,
}

pub fn calculate_size(keys: &BTreeMap<u64, RangeIndexKey>, col_byte_sizes: &Vec<u8>) -> usize {
    let key_size = *col_byte_sizes.get(0).unwrap();
    let mut size = 4 + key_size as usize * 2;

    if col_byte_sizes.len() > 1 {
        for (k, v) in keys {
            size += get_size_into(v, col_byte_sizes, 0);
        }
    } else {
        for (k, v) in keys {
            size += key_size as usize + 8;
        }
    }

    size
}

pub fn get_size_into(key: &RangeIndexKey, col_byte_sizes: &Vec<u8>, depth: usize) -> usize {
    if depth == col_byte_sizes.len() {
        let key_size = *col_byte_sizes.get(depth - 1).unwrap();
        key_size as usize + 8
    } else {
        //println!("depth {} len {}", depth, col_byte_sizes.len());
        let key_size = *col_byte_sizes.get(depth).unwrap();
        let mut size = 4 + key_size as usize * 2;

        for k in key.secondary_keys.as_ref().unwrap() {
            size += get_size_into(key, col_byte_sizes, depth + 1);
            size += 8;
        }

        size
    }
}
/// Unique endless depth range index for composite numeric primary key [u16, u32, u64]
/// first bytes meta header
/// data from offset 1024
///
/// first layer keys
/// offset     | value
/// 0x00-0x03  | keys count u32
/// 0x04-0xXX  | from key bytes with size_of<Type>
/// 0xXX-0xXX  | to key bytes with size_of<Type>
/// 0xXX-0xXX  | key 0
/// 0xXX-0xXX  | key 1
/// ..................
/// 0xXX-0xXX  | last key
///
/// ----
/// offset      | k-v
/// 0x01-0x0N   | key1 [u8]    
/// 0x0N-0xXX   | offset 1 u64 -- offset in meta struct OR offset or next layer offset for key
/// ......................
/// 0xXX-0xXN   | key N u64    
/// 0xXX-0xXX   | offset N u64    
/// ......................
/// offset_1    |
/// 0x00-0x03   | keys count u32
/// 0x04-0xXX   | from key bytes with size_of<Type>
/// 0xXX-0xXX   | to key bytes with size_of<Type>
/// 0xXX-0xXX   | next layer key1 [u8]    
/// 0xXX-0xXX   | offset 1 u64 -- offset in meta struct OR offset or next layer offset for key
/// ......................
/// 0xXX-0xXX   | key N u64    
/// 0xXX-0xXX   | offset N u64    
/// ......................
/// example
/// pk in two numbers <u64, u32>
/// [1,21]
/// [1,33]
/// [2,55]
/// [2,66]
/// [2,99]
///
/// offset      |
/// 0xXX        | keys count u32 2 -- first uniq keys [1,2]
/// 0xXX        | key from -- 1 as u64 8 bytes
/// 0xXX        | key to -- 2 as u64 8 bytes
/// 0xXX        | key 1
/// 0xXX        | offset of range key1_sec_layer_offset
/// 0xXX        | key 2
/// 0xXX        | offset of range key2_sec_layer_offset
/// key1_sec_layer_offset |
/// 0xXX        | keys count u32 2 -- keys [21,33]
/// 0xXX        | key from -- 21 as u32 4 bytes
/// 0xXX        | key to -- 33 as u32 4 bytes
/// 0xXX        | key 21
/// 0xXX        | offset in meta table
/// 0xXX        | key 33
/// 0xXX        | offset in meta table
/// key2_sec_layer_offset |
/// 0xXX        | keys count u32 2 -- keys [55,66,99]
/// 0xXX        | key from -- 55 as u32 4 bytes
/// 0xXX        | key to -- 99 as u32 4 bytes
/// 0xXX        | key 55
/// 0xXX        | offset in meta table
/// 0xXX        | key 66
/// 0xXX        | offset in meta table
/// 0xXX        | key 99
/// 0xXX        | offset in meta table

impl RangeIdxStruct {
    pub fn new_with_data_mmap(
        path: &String,
        header: &EntityHeader,
        segment_id: u32,
        columns: &Vec<ColumnDescription>,
        keys: &BTreeMap<u64, RangeIndexKey>,
        key_offsets: &Box<dyn PkIdx + Sync + Send>,
    ) -> Result<Self> {
        let col_byte_sizes: Vec<u8> = columns.iter().map(|c| c.data_type.bytes_size).collect();

        let file_size = calculate_size(keys, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;

        let mut file = MmapStruct::new_limited(path, file_size as u64)?;
        header.save_to_mapped_file(&mut file, 0);

        let mut offset = ENTITY_HEADER_LENGTH;
        file.put_u32(offset, segment_id);
        offset += 8;

        let keys_bitmap: u64 = pk_keys_bitmap(columns);

        //write keys

        let col_types: Vec<&DataType> = columns.iter().map(|c| c.data_type).collect();

        store_segment_keys(
            &mut file,
            keys,
            key_offsets,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let mut from_vec: Vec<u64> = vec![0; columns.len()];
        let mut to_vec: Vec<u64> = vec![0; columns.len()];

        //find range

        let mut first = keys.first_key_value().unwrap().1;
        let mut last = keys.last_key_value().unwrap().1;

        for i in 0..columns.len() {
            from_vec[i] = first.from;
            to_vec[i] = last.to;

            if i < columns.len() - 1 {
                first = first
                    .secondary_keys
                    .as_ref()
                    .unwrap()
                    .first_key_value()
                    .unwrap()
                    .1;
                last = last
                    .secondary_keys
                    .as_ref()
                    .unwrap()
                    .last_key_value()
                    .unwrap()
                    .1;
            }
        }

        let mut offset: usize = RANGE_PK_OFFSET;
        for f in &from_vec {
            file.put_u64(offset, *f);
            offset += 8;
        }

        for t in &to_vec {
            file.put_u64(offset, *t);
            offset += 8;
        }

        put_crc(&mut file);
        file.flush()?;

        let from_num = from_vec.get(0).unwrap();
        let to_num = to_vec.get(0).unwrap();

        Ok(RangeIdxStruct {
            mapped_file: file,
            header: header.clone(),
            segment_id: segment_id,
            keys_bitmap,
            columns: columns.iter().map(|c| c.col_id).collect(),
            range: PkRange {
                pk_num_from: *from_num,
                pk_num_to: *to_num,
                pk_composite_from: from_vec,
                pk_composite_to: to_vec,
                pk_bytes_from: vec![],
                pk_bytes_to: vec![],
                single_num: false,
            },
        })
    }

    pub fn load(path: &String, columns: &Vec<ColumnDescription>, keys_bitmap: u64) -> Result<Self> {
        let mut file = MmapStruct::load_mlock(path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let mut offset = ENTITY_HEADER_LENGTH;
        let segment_id = file.get_u32(offset);

        let mut from_vec: Vec<u64> = vec![0; columns.len()];
        let mut to_vec: Vec<u64> = vec![0; columns.len()];

        let mut offset: usize = RANGE_PK_OFFSET;
        for i in 0..columns.len() {
            from_vec[i] = file.get_u64(offset);
            offset += 8;
        }

        for i in 0..columns.len() {
            to_vec[i] = file.get_u64(offset);
            offset += 8;
        }

        let from_num = *from_vec.get(0).unwrap();
        let to_num = *to_vec.get(0).unwrap();

        Ok(RangeIdxStruct {
            mapped_file: file,
            header,
            segment_id: segment_id,
            keys_bitmap,
            columns: columns.iter().map(|c| c.col_id).collect(),
            range: PkRange {
                pk_num_from: from_num,
                pk_num_to: to_num,
                pk_composite_from: from_vec,
                pk_composite_to: to_vec,
                pk_bytes_from: vec![],
                pk_bytes_to: vec![],
                single_num: false,
            },
        })
    }

    pub fn find_exact_secondary(
        &self,
        pk: u64,
        sec_key: u64,
        condition: &QueryConditionType,
        limit: u64,
        offset: u64,
    ) -> Option<Vec<u64>> {
        let mmap = &self.mapped_file;

        let mut keys_offset: usize = RANGE_IDX_DATA_OFFSET;

        let pk_len = mmap.get_u64(keys_offset) as usize;
        let pk_from = mmap.get_u64(keys_offset + 8) as u64;
        let pk_to = mmap.get_u64(keys_offset + 16) as u64;

        println!(
            "pk len {} first {} last {} pk {}",
            pk_len, pk_from, pk_to, pk
        );

        if (pk < pk_from || pk > pk_to) {
            println!("pk fail");
            return Option::None;
        }

        keys_offset += 24;

        //todo binary search

        for i in 0..pk_len {
            let pk_val = mmap.get_u64(keys_offset);
            println!("pk val {} offset {}", pk_val, keys_offset);

            if (pk_val == pk) {
                let sec_from = mmap.get_u64(keys_offset + 8);
                let sec_to = mmap.get_u64(keys_offset + 16);

                println!("sec first {} last {} pk {}", sec_from, sec_to, sec_key);
                if (sec_key < sec_from || sec_key > sec_to) {
                    return Option::None;
                }

                let mut sec_offset = mmap.get_u64(keys_offset + 24);
                let sec_len = mmap.get_u64(sec_offset as usize);
                sec_offset += 8;

                for s in 0..sec_len {
                    let val = mmap.get_u64(sec_offset as usize);

                    if (val == sec_key) {
                        return Option::Some(vec![pk]);
                    }
                    sec_offset += 8;
                }
            }
            keys_offset += 32;
        }

        Option::None
    }

    pub fn find_range(
        &self,
        keys: &Vec<u64>,
        conditions: &Vec<QueryConditionType>,
        col_types: &Vec<&DataType>,
        offset: usize,
        depth: usize,
        depth_limit: usize,
        limit: u32,
        offset_: u32,
    ) -> Vec<u64> {
        let bytes_size = col_types.get(depth as usize).unwrap().bytes_size;

        let file = &self.mapped_file;
        let mut offset = offset;

        let size = self.mapped_file.get_u32(offset);
        offset += 4;
        let from = read_val(file, offset, bytes_size);
        offset += bytes_size as usize;
        let to = read_val(file, offset, bytes_size);
        offset += bytes_size as usize;

        let key = *keys.get(depth).unwrap();
        let c = conditions.get(depth).unwrap();

        let mut values: Vec<u64> = vec![];

        if matches!(c, QueryConditionType::EQ) {
            let pos = find_eq_position_offset(file, key, bytes_size, offset, size as usize);
            if pos.is_some() {
                let offset = pos.unwrap() as usize; //offset + pos.unwrap()as usize  * (bytes_size as usize + 8);
                if depth == depth_limit - 1 {
                    values.push(pos.unwrap());
                } else {
                    values.extend(find_range(
                        file,
                        keys,
                        conditions,
                        col_types,
                        offset,
                        depth + 1,
                        depth_limit,
                        limit,
                        offset_,
                    ));
                }
            }
        } else {
            let res = find_position(file, key, c, bytes_size, offset, size as usize);
            if res.is_some() {
                let (start, end, reverse) = match c {
                    QueryConditionType::GT | QueryConditionType::GTE => {
                        (res.unwrap() as usize, size as usize, false)
                    }
                    QueryConditionType::LT | QueryConditionType::LTE => {
                        (0 as usize, res.unwrap() as usize + 1, true)
                    }
                    _ => (0, 0, false),
                };

                if !reverse {
                    for i in start..end {
                        let key_offset = offset + i * (8 + bytes_size as usize);
                        let key = read_val(file, key_offset, bytes_size);
                        let val = file.get_u64(key_offset + bytes_size as usize);

                        if depth == depth_limit - 1 {
                            values.push(val);
                        } else {
                            values.extend(find_range(
                                file,
                                keys,
                                conditions,
                                col_types,
                                val as usize,
                                depth + 1,
                                depth_limit,
                                limit,
                                offset_,
                            ));
                        }
                    }
                } else {
                    for i in (start..end).rev() {
                        let key_offset = offset + i * (8 + bytes_size as usize);
                        let key = read_val(file, key_offset, bytes_size);
                        let val = file.get_u64(key_offset + bytes_size as usize);

                        if depth == depth_limit - 1 {
                            values.push(val);
                        } else {
                            values.extend(find_range(
                                file,
                                keys,
                                conditions,
                                col_types,
                                val as usize,
                                depth + 1,
                                depth_limit,
                                limit,
                                offset_,
                            ));
                        }
                    }
                }
            }
        }

        values
    }
}

pub struct RangeIdxSegmentHolder<'a> {
    pub idx: &'a RangeIdxStruct,
}

impl<'a> RangeIdxSegmentHolder<'a> {
    pub fn iter(&mut self) -> RangeIdxSegmentIterator<'_> {
        RangeIdxSegmentIterator {
            idx: self.idx,
            curr: NumericPrimaryKey {
                key_vector: self.idx.range.pk_composite_from.clone(),
            },
        }
    }
}

pub struct RangeIdxSegmentIterator<'a> {
    idx: &'a RangeIdxStruct,
    curr: NumericPrimaryKey,
}

impl<'a> Iterator for RangeIdxSegmentIterator<'a> {
    type Item = NumericPrimaryKey;

    fn next(&mut self) -> Option<Self::Item> {
        Option::Some(self.curr.clone())
    }
}

fn write_val(file: &mut MmapStruct, offset: usize, value: u64, key_size: u8) -> Result<()> {
    let size = file.file_size;
    if offset > size as usize {
        println!("write idx more offset {}  size {}", offset, size);
        panic!()
    }
    if key_size == 8 {
        file.put_u64(offset, value);
    } else if key_size == 4 {
        file.put_u32(offset, value as u32);
    } else if key_size == 2 {
        file.put_u16(offset, value as u16);
    } else {
        bail!("unsupported PK type")
    };

    Ok(())
}

/*
fn store_keys(file: &mut MemMappedStruct, keys: &BTreeMap<u64, RangeIndexKey>, col_types: &Vec<&DataType>, offset: usize, depth: usize) -> usize {
    let key_size = col_types.get(depth).unwrap().bytes_size;

    let mut offset: usize = offset;

    file.put_u32(offset, keys.len() as u32);
    offset += 4;

    write_val(file, offset, *keys.first_key_value().unwrap().0, key_size);
    offset += key_size as usize;
    write_val(file, offset, *keys.last_key_value().unwrap().0, key_size);
    offset += key_size as usize;

    //println!("next pk offset {}, from {} , to {}", offset, keys.first().unwrap().primary, keys.last().unwrap().primary);
    let size = (key_size as usize + 8) * keys.len();
    let mut sec_offset = size + offset;

    let final_layer = depth == col_types.len() - 1;

    for (k, v) in keys {
        write_val(file, offset, *k, key_size);
        offset += key_size as usize;

        if final_layer {
            file.put_u64(offset, 0); //todo
            offset += 8;
        } else {
            file.put_u64(offset, sec_offset as u64);
            offset += 8;

            sec_offset = store_keys(file, v.secondary_keys.as_ref().unwrap(), col_types, sec_offset, depth + 1);
        }

    }
    sec_offset
}
*/

fn store_segment_keys(
    file: &mut MmapStruct,
    keys: &BTreeMap<u64, RangeIndexKey>,
    key_offsets: &Box<dyn PkIdx + Sync + Send>,
    col_types: &Vec<&DataType>,
    offset: usize,
    depth: usize,
) -> usize {
    let key_size = col_types.get(depth).unwrap().bytes_size;

    let mut offset: usize = offset;

    file.put_u32(offset, keys.len() as u32);
    offset += 4;

    write_val(file, offset, *keys.first_key_value().unwrap().0, key_size);
    offset += key_size as usize;
    write_val(file, offset, *keys.last_key_value().unwrap().0, key_size);
    offset += key_size as usize;

    //println!("next pk offset {}, from {} , to {}", offset, keys.first().unwrap().primary, keys.last().unwrap().primary);
    let size = (key_size as usize + 8) * keys.len();
    let mut sec_offset = size + offset;

    let final_layer = depth == col_types.len() - 1;

    for (k, v) in keys {
        write_val(file, offset, *k, key_size);
        offset += key_size as usize;

        if final_layer {
            let key_offset = key_offsets.get(&v.value.as_ref().unwrap().pk);
            file.put_u64(offset, key_offset);
            offset += 8;
        } else {
            file.put_u64(offset, sec_offset as u64);
            offset += 8;

            sec_offset = store_segment_keys(
                file,
                v.secondary_keys.as_ref().unwrap(),
                key_offsets,
                col_types,
                sec_offset,
                depth + 1,
            );
        }
    }
    sec_offset
}

fn read_val(file: &MmapStruct, offset: usize, bytes_size: u8) -> u64 {
    if bytes_size == 8 {
        file.get_u64(offset)
    } else if bytes_size == 4 {
        file.get_u32(offset) as u64
    } else if bytes_size == 2 {
        file.get_u16(offset) as u64
    } else {
        0
    }
}

fn find_eq_position_offset(
    file: &MmapStruct,
    key: u64,
    bytes_size: u8,
    offset: usize,
    length: usize,
) -> Option<u64> {
    let mut low: usize = 0;
    let mut high = length - 1;
    while (low <= high) {
        let mid = low + (high - low) / 2;
        let median_offset = offset + mid * (8 + bytes_size as usize);
        let k = read_val(file, median_offset, bytes_size);
        if (k == key) {
            return Option::Some(file.get_u64(median_offset + bytes_size as usize));
        }

        if k < key {
            low = mid + 1;
        } else {
            if mid == 0 {
                return Option::None;
            }

            high = mid - 1;
        }
    }

    Option::None
}

fn find_position(
    file: &MmapStruct,
    key: u64,
    c: &QueryConditionType,
    bytes_size: u8,
    offset: usize,
    length: usize,
) -> Option<usize> {
    let mut low: usize = 0;
    let mut high = length - 1;
    let mut last_pos: Option<usize> = Option::None;
    while (low <= high) {
        let mid = low + (high - low) / 2;
        let median_offset = offset + mid * (8 + bytes_size as usize);
        let k = read_val(file, median_offset, bytes_size);

        match c {
            QueryConditionType::GT => {
                if k > key {
                    last_pos = Option::Some(mid);
                    if mid == 0 {
                        return last_pos;
                    } else {
                        high = mid - 1;
                    }
                } else {
                    low = mid + 1;
                }
            }
            QueryConditionType::LT => {
                if k < key {
                    last_pos = Option::Some(mid);
                    low = mid + 1;
                } else {
                    if mid == 0 {
                        return last_pos;
                    } else {
                        high = mid - 1;
                    }
                }
            }
            QueryConditionType::GTE => {
                if k >= key {
                    last_pos = Option::Some(mid);
                    if mid == 0 {
                        return last_pos;
                    } else {
                        high = mid - 1;
                    }
                } else {
                    low = mid + 1;
                }
            }
            QueryConditionType::LTE => {
                if k <= key {
                    last_pos = Option::Some(mid);
                    low = mid + 1;
                } else {
                    if mid == 0 {
                        return last_pos;
                    } else {
                        high = mid - 1;
                    }
                }
            }
            _ => return Option::None,
        }
    }

    last_pos
}

fn find_key(
    file: &MmapStruct,
    keys: &Vec<u64>,
    col_types: &Vec<&DataType>,
    offset: usize,
    depth: usize,
    depth_limit: usize,
) -> Option<u64> {
    let bytes_size = col_types.get(depth as usize).unwrap().bytes_size;

    let key = *keys.get(depth).unwrap();

    let mut offset = offset;

    let size = file.get_u32(offset);
    offset += 4;
    let from = read_val(file, offset, bytes_size);
    offset += bytes_size as usize;
    let to = read_val(file, offset, bytes_size);
    offset += bytes_size as usize;

    if key < from || key > to {
        return Option::None;
    }

    let pos = find_eq_position_offset(file, key, bytes_size, offset, size as usize);
    if pos.is_some() {
        if depth == depth_limit - 1 {
            return Option::Some(pos.unwrap());
        } else {
            return find_key(
                file,
                &keys,
                col_types,
                pos.unwrap() as usize,
                depth + 1,
                depth_limit,
            );
        }
    }

    Option::None
}

fn find_range(
    file: &MmapStruct,
    keys: &Vec<u64>,
    conditions: &Vec<QueryConditionType>,
    col_types: &Vec<&DataType>,
    offset: usize,
    depth: usize,
    depth_limit: usize,
    limit: u32,
    offset_: u32,
) -> Vec<u64> {
    let bytes_size = col_types.get(depth as usize).unwrap().bytes_size;

    let mut offset = offset;

    let size = file.get_u32(offset);
    offset += 4;
    let from = read_val(file, offset, bytes_size);
    offset += bytes_size as usize;
    let to = read_val(file, offset, bytes_size);
    offset += bytes_size as usize;

    let key = *keys.get(depth).unwrap();
    let c = conditions.get(depth).unwrap();

    let mut values: Vec<u64> = vec![];

    if matches!(c, QueryConditionType::EQ) {
        let pos = find_eq_position_offset(file, key, bytes_size, offset, size as usize);
        if pos.is_some() {
            let offset = pos.unwrap() as usize; //offset + pos.unwrap()as usize  * (bytes_size as usize + 8);
            if depth == depth_limit - 1 {
                values.push(pos.unwrap());
            } else {
                values.extend(find_range(
                    file,
                    keys,
                    conditions,
                    col_types,
                    offset,
                    depth + 1,
                    depth_limit,
                    limit,
                    offset_,
                ));
            }
        }
    } else {
        let res = find_position(file, key, c, bytes_size, offset, size as usize);
        if res.is_some() {
            let (start, end, reverse) = match c {
                QueryConditionType::GT | QueryConditionType::GTE => {
                    (res.unwrap() as usize, size as usize, false)
                }
                QueryConditionType::LT | QueryConditionType::LTE => {
                    (0 as usize, res.unwrap() as usize + 1, true)
                }
                _ => (0, 0, false),
            };

            if !reverse {
                for i in start..end {
                    let key_offset = offset + i * (8 + bytes_size as usize);
                    let key = read_val(file, key_offset, bytes_size);
                    let val = file.get_u64(key_offset + bytes_size as usize);

                    if depth == depth_limit - 1 {
                        values.push(val);
                    } else {
                        values.extend(find_range(
                            file,
                            keys,
                            conditions,
                            col_types,
                            val as usize,
                            depth + 1,
                            depth_limit,
                            limit,
                            offset_,
                        ));
                    }
                }
            } else {
                for i in (start..end).rev() {
                    let key_offset = offset + i * (8 + bytes_size as usize);
                    let key = read_val(file, key_offset, bytes_size);
                    let val = file.get_u64(key_offset + bytes_size as usize);

                    if depth == depth_limit - 1 {
                        values.push(val);
                    } else {
                        values.extend(find_range(
                            file,
                            keys,
                            conditions,
                            col_types,
                            val as usize,
                            depth + 1,
                            depth_limit,
                            limit,
                            offset_,
                        ));
                    }
                }
            }
        }
    }

    values
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, sync::Arc};

    use crate::{
        column_descr,
        data::{
            MmapStruct, U64OAHashMapIdx,
            index::{
                CompositePkOAHashMapIdx, PkIdx, PrimaryKey, RANGE_IDX_DATA_OFFSET, RangeIdxStruct,
                RangeIndexKey, calculate_size, find_eq_position_offset, find_key, find_position,
                find_range, range_idx_select_conditions, range_idx_select_eq, store_segment_keys,
                write_val,
            },
            segment::{ColumnDescription, PkRange, SegmentMetaStruct, VarLenStruct},
        },
        db::state::get_current_timestamp_millis,
        model::{
            Column,
            column::Row,
            data_type::{BLOB, DataType, INT, JSON, LONG, LONG_VECTOR, TIMESTAMP},
        },
        new_column,
        protocol::QueryConditionType,
    };

    const IDX_PATH: &'static str = "/tmp/idx";
    const RAW_FILE_PATH: &'static str = "/tmp/idx_raw";

    #[test]
    fn test_oa_map() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());

        let count = 7282 as u64;

        let mut map = U64OAHashMapIdx::new(&idx_name, count as u32).unwrap();

        for i in 0..count {
            map.put(i, i);
        }

        map.flush();

        let mut map = U64OAHashMapIdx::load(&idx_name).unwrap();
        for i in 0..count {
            let val = map.get(i);
            assert_eq!(val, i);
        }

        let keys = map.get_keys();

        for k in keys {
            let val = map.get(k);
            assert_eq!(val, k);
        }

        /*
        map.put(100, 200);
        map.put(54312545245, 300);
        map.put(135243564, 34534534);
        map.close();

        assert_eq!(map.get(100), 200);
        assert_eq!(map.get(54312545245), 300);
        assert_eq!(map.get(135243564), 34534534);

        let mut map = LongLongOAHashMap::open("/tmp/ll_map".to_string());
        assert_eq!(map.keys_count, 3);
        assert_eq!(map.get(100), 200);
        assert_eq!(map.get(54312545245), 300);
        assert_eq!(map.get(135243564), 34534534);
        */

        fs::remove_file(idx_name);
    }

    //#[test]
    fn test_load_map() {
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(
            0x0,
            0x1,
            0,
            &LONG_VECTOR,
            "long_vec".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x1,
            0x0,
            4,
            &JSON,
            "varl_c".as_bytes().to_vec()
        ));

        let mut map = U64OAHashMapIdx::load(
            &"/tmp/db/ks1-1000/blobs-1756914914174/1757004343235.idx".to_string(),
        )
        .unwrap();
        let mut varl = VarLenStruct::load(
            &"/tmp/db/ks1-1000/blobs-1756914914174/bytes-1757004343235.var".to_string(),
        )
        .unwrap();
        let mut meta = SegmentMetaStruct::load(
            &"/tmp/db/ks1-1000/blobs-1756914914174/segment_meta_1757004343235.db".to_string(),
            &columns,
        )
        .unwrap();

        let base: u64 = 1757004345932;
        println!("map size {}", map.get_keys().len());
        for i in 0..1000 {
            let offset = map.get(i + base);
            //println!("offset {}", offset);
            let row_meta = meta.get_row_meta(offset);
            for v in row_meta.var_len_rows {
                let b = varl.read_bytes(v.file_offset as u32);
                if (b.len() > 100) {
                    //println!("key {} offset {}", i, offset); //key 1757001706789 offset 24580144
                }
            }
            //println!("meta {}", row_meta.idx);
        }
    }

    #[test]
    fn test_composite_map_2() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());

        let count = 20 as u64;
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "col2".as_bytes().to_vec()));

        let mut map = CompositePkOAHashMapIdx::new(&idx_name, count as u32, &columns).unwrap();

        let mut keys: Vec<PrimaryKey> = vec![];
        for i in 0..count {
            let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
            let col2 = new_column!(
                1,
                LONG.id,
                0,
                0x1,
                u32::to_le_bytes(i as u32 + 1000).to_vec()
            );
            let pk = PrimaryKey {
                primary: col,
                secondary: Option::Some(vec![col2]),
            };
            map.put(&pk, i);
            keys.push(pk);
        }

        map.flush();

        let mut map = CompositePkOAHashMapIdx::load(&idx_name).unwrap();
        for pk in keys {
            let val = map.get(&pk);
            assert_eq!(val, pk.primary.num_val);
        }

        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_composite_map_3() {
        let count = 345 as u64;
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());

        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));
        columns.push(column_descr!(0x2, 0x1, 0, &INT, "pk3".as_bytes().to_vec()));

        let mut map = CompositePkOAHashMapIdx::new(&idx_name, count as u32, &columns).unwrap();

        let mut keys: Vec<PrimaryKey> = vec![];
        for i in 0..count {
            let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
            let col2 = new_column!(
                1,
                INT.id,
                0,
                0x1,
                u32::to_le_bytes(i as u32 + 1000).to_vec()
            );
            let col3 = new_column!(
                2,
                INT.id,
                0,
                0x1,
                u32::to_le_bytes(i as u32 + 1000_000).to_vec()
            );

            let pk = PrimaryKey {
                primary: col,
                secondary: Option::Some(vec![col2, col3]),
            };
            map.put(&pk, i);
            keys.push(pk);
        }

        map.flush();

        let mut map = CompositePkOAHashMapIdx::load(&idx_name).unwrap();
        for pk in keys {
            let val = map.get(&pk);
            assert_eq!(val, pk.primary.num_val);
        }
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_recursive_mmap_idx_single_layer() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> =
            Box::new(U64OAHashMapIdx::new(&idx_name, 1024 as u32).unwrap());

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG];

        for i in 1..10 {
            let pk_col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
            let pk = PrimaryKey {
                primary: pk_col,
                secondary: Option::None,
            };
            pk_idx.put(&pk, i);
            let row = Row {
                pk: pk,
                timestamp: 0,
                tx_id: 1,
                payload: vec![],
                deleted: false,
            };
            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 0,
                    to: 0,
                    secondary_keys: Option::None,
                    value: Option::Some(Arc::new(row)),
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        println!(
            "calculated size single {} test_recursive_mmap_idx_single_layer {:?}",
            file_size, col_byte_sizes
        );
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();
        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![7];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 7);

        let keys_vec: Vec<u64> = vec![12];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 1);
        assert!(result.is_none());

        let keys_vec: Vec<u64> = vec![0];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 1);
        assert!(result.is_none());

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    //BACKTRACE=1 RUSTFLAGS="-Awarnings" cargo test test_recursive_mmap_idx_single_layer -- --nocapture

    #[test]
    fn test_recursive_mmap_idx_2_layer() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));

        let mut pk_map = CompositePkOAHashMapIdx::new(&idx_name, 1024, &columns).unwrap();

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> = Box::new(pk_map);

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG, &INT];

        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 1000..1005 {
                let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());

                let pk = PrimaryKey {
                    primary: col,
                    secondary: Option::Some(vec![col2]),
                };
                pk_idx.put(&pk, s);
                let row = Row {
                    pk: pk,
                    timestamp: 0,
                    tx_id: 1,
                    payload: vec![],
                    deleted: false,
                };
                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 0,
                        to: 0,
                        secondary_keys: Option::None,
                        value: Option::Some(Arc::new(row)),
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 1000,
                    to: 1005,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        println!(
            "calculated size {} test_recursive_mmap_idx_3_layer {:?}",
            file_size, col_byte_sizes
        );
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![2, 1001];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 2);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1001);

        let keys_vec: Vec<u64> = vec![9, 1004];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 2);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1004);

        let keys_vec: Vec<u64> = vec![11, 1004];
        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 2);
        assert!(result.is_none());

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_recursive_mmap_idx_3_layer() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));
        columns.push(column_descr!(0x2, 0x1, 0, &INT, "pk3".as_bytes().to_vec()));
        let mut pk_map = CompositePkOAHashMapIdx::new(&idx_name, 1024, &columns).unwrap();

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> = Box::new(pk_map);

        let col_types: Vec<&DataType> = vec![&LONG, &INT, &INT];

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 20..25 {
                let mut third_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
                for t in 30..35 {
                    let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                    let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                    let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(t as u32).to_vec());

                    let pk = PrimaryKey {
                        primary: col,
                        secondary: Option::Some(vec![col2, col3]),
                    };
                    pk_idx.put(&pk, t);
                    let row = Row {
                        pk: pk,
                        timestamp: 0,
                        tx_id: 1,
                        payload: vec![],
                        deleted: false,
                    };
                    third_layer.insert(
                        t,
                        RangeIndexKey {
                            primary: t,
                            from: 0,
                            to: 0,
                            secondary_keys: Option::None,
                            value: Option::Some(Arc::new(row)),
                        },
                    );
                }

                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 30,
                        to: 35,
                        secondary_keys: Option::Some(third_layer),
                        value: Option::None,
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 20,
                    to: 25,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        println!(
            "calculated size {} test_recursive_mmap_idx_3_layer {:?}",
            file_size, col_byte_sizes
        );

        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![1, 20, 30];

        let result = find_key(&file, &keys_vec, &col_types, RANGE_IDX_DATA_OFFSET, 0, 3);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), 30);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_binary_search_eq() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut file = MmapStruct::new_limited(&row_name, 1024 * 8).unwrap();

        let len: usize = 9;
        for i in 0..len {
            write_val(&mut file, i * 16, i as u64 + 1000, 8);
            write_val(&mut file, i * 16 + 8, i as u64 + 2000, 8);
        }

        let result = find_eq_position_offset(&file, 1001, 8, 0, len);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 2001);

        let result = find_eq_position_offset(&file, 100, 8, 0, len);
        assert!(result.is_none());

        let result = find_eq_position_offset(&file, 10000, 8, 0, len);
        assert!(result.is_none());

        let result = find_eq_position_offset(&file, 1008, 8, 0, len);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 2008);

        let result = find_eq_position_offset(&file, 1002, 8, 0, len);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 2002);

        let result = find_eq_position_offset(&file, 1000, 8, 0, len);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 2000);

        fs::remove_file(&row_name);
    }

    #[test]
    fn test_binary_search_with_condition() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut file = MmapStruct::new_limited(&row_name, 1024 * 8).unwrap();

        let len: usize = 15;
        for i in 0..len {
            write_val(&mut file, i * 16, i as u64 + 1000, 8);
            write_val(&mut file, i * 16 + 8, i as u64 + 2000, 8);
        }

        let result = find_position(&file, 1002, &QueryConditionType::LT, 8, 0, len);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1);

        let result = find_position(&file, 1011, &QueryConditionType::GT, 8, 0, 10);
        assert!(result.is_none());

        let result = find_position(&file, 1011, &QueryConditionType::GTE, 8, 0, 10);
        assert!(result.is_none());

        let result = find_position(&file, 999, &QueryConditionType::LTE, 8, 0, 10);
        assert!(result.is_none());

        let result = find_position(&file, 1001, &QueryConditionType::GT, 8, 0, 10);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 2);

        let result = find_position(&file, 1005, &QueryConditionType::LT, 8, 0, 10);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 4);

        let result = find_position(&file, 1005, &QueryConditionType::LTE, 8, 0, 10);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 5);

        let result = find_position(&file, 1005, &QueryConditionType::GTE, 8, 0, 10);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 5);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_single() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut pk_idx: Box<dyn PkIdx + Sync + Send> =
            Box::new(U64OAHashMapIdx::new(&idx_name, 1024 as u32).unwrap());

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG];
        let conditions: Vec<QueryConditionType> = vec![QueryConditionType::EQ];

        for i in 0..10 {
            let pk_col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());

            let pk = PrimaryKey {
                primary: pk_col,
                secondary: Option::None,
            };
            pk_idx.put(&pk, i);
            let row = Row {
                pk: pk,
                timestamp: 0,
                tx_id: 1,
                payload: vec![],
                deleted: false,
            };
            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 0,
                    to: 0,
                    secondary_keys: Option::None,
                    value: Option::Some(Arc::new(row)),
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![7];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            1,
            0,
            0,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(*result.get(0).unwrap(), 7);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_single_cond() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut pk_idx: Box<dyn PkIdx + Sync + Send> =
            Box::new(U64OAHashMapIdx::new(&idx_name, 1024 as u32).unwrap());

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG];

        for i in 0..10 {
            let pk_col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());

            let pk = PrimaryKey {
                primary: pk_col,
                secondary: Option::None,
            };
            pk_idx.put(&pk, i);
            let row = Row {
                pk: pk,
                timestamp: 0,
                tx_id: 1,
                payload: vec![],
                deleted: false,
            };
            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 0,
                    to: 0,
                    secondary_keys: Option::None,
                    value: Option::Some(Arc::new(row)),
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![5];
        let conditions: Vec<QueryConditionType> = vec![QueryConditionType::LT];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            1,
            0,
            0,
        );
        assert_eq!(result.len(), 5);
        assert_eq!(*result.get(0).unwrap(), 4);
        assert_eq!(*result.get(1).unwrap(), 3);
        assert_eq!(*result.get(2).unwrap(), 2);
        assert_eq!(*result.get(3).unwrap(), 1);
        assert_eq!(*result.get(4).unwrap(), 0);

        let keys_vec: Vec<u64> = vec![2];
        let conditions: Vec<QueryConditionType> = vec![QueryConditionType::LTE];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            1,
            0,
            0,
        );
        assert_eq!(result.len(), 3);
        assert_eq!(*result.get(0).unwrap(), 2);
        assert_eq!(*result.get(1).unwrap(), 1);
        assert_eq!(*result.get(2).unwrap(), 0);

        let keys_vec: Vec<u64> = vec![7];
        let conditions: Vec<QueryConditionType> = vec![QueryConditionType::GT];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            1,
            0,
            0,
        );
        assert_eq!(result.len(), 2);
        assert_eq!(*result.get(0).unwrap(), 8);
        assert_eq!(*result.get(1).unwrap(), 9);

        let keys_vec: Vec<u64> = vec![7];
        let conditions: Vec<QueryConditionType> = vec![QueryConditionType::GTE];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            1,
            0,
            0,
        );
        assert_eq!(result.len(), 3);
        assert_eq!(*result.get(0).unwrap(), 7);
        assert_eq!(*result.get(1).unwrap(), 8);
        assert_eq!(*result.get(2).unwrap(), 9);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_idx_2_layer() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let conditions: Vec<QueryConditionType> =
            vec![QueryConditionType::EQ, QueryConditionType::EQ];

        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));

        let mut pk_map = CompositePkOAHashMapIdx::new(&idx_name, 1024, &columns).unwrap();

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> = Box::new(pk_map);

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG, &INT];

        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 1000..1005 {
                let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                let pk = PrimaryKey {
                    primary: col,
                    secondary: Option::Some(vec![col2]),
                };
                pk_idx.put(&pk, s);
                let row = Row {
                    pk: pk,
                    timestamp: 0,
                    tx_id: 1,
                    payload: vec![],
                    deleted: false,
                };
                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 0,
                        to: 0,
                        secondary_keys: Option::None,
                        value: Option::Some(Arc::new(row)),
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 1000,
                    to: 1005,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let keys_vec: Vec<u64> = vec![2, 1003];

        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            2,
            0,
            0,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(*result.get(0).unwrap(), 1003);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_idx_2_layer_cond() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));

        let mut pk_map = CompositePkOAHashMapIdx::new(&idx_name, 1024, &columns).unwrap();

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> = Box::new(pk_map);

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();

        let col_types: Vec<&DataType> = vec![&LONG, &INT];

        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 1000..1005 {
                let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                let pk = PrimaryKey {
                    primary: col,
                    secondary: Option::Some(vec![col2]),
                };
                pk_idx.put(&pk, s);
                let row = Row {
                    pk: pk,
                    timestamp: 0,
                    tx_id: 1,
                    payload: vec![],
                    deleted: false,
                };
                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 0,
                        to: 0,
                        secondary_keys: Option::None,
                        value: Option::Some(Arc::new(row)),
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 1000,
                    to: 1005,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let conditions: Vec<QueryConditionType> =
            vec![QueryConditionType::EQ, QueryConditionType::LT];
        let keys_vec: Vec<u64> = vec![2, 1002];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            2,
            0,
            0,
        );
        assert_eq!(result.len(), 2);
        assert_eq!(*result.get(0).unwrap(), 1001);
        assert_eq!(*result.get(1).unwrap(), 1000);

        let conditions: Vec<QueryConditionType> =
            vec![QueryConditionType::EQ, QueryConditionType::LTE];
        let keys_vec: Vec<u64> = vec![2, 1002];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            2,
            0,
            0,
        );
        assert_eq!(result.len(), 3);
        assert_eq!(*result.get(0).unwrap(), 1002);
        assert_eq!(*result.get(1).unwrap(), 1001);
        assert_eq!(*result.get(2).unwrap(), 1000);

        let conditions: Vec<QueryConditionType> =
            vec![QueryConditionType::EQ, QueryConditionType::GT];
        let keys_vec: Vec<u64> = vec![2, 1002];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            2,
            0,
            0,
        );
        assert_eq!(result.len(), 2);
        assert_eq!(*result.get(0).unwrap(), 1003);
        assert_eq!(*result.get(1).unwrap(), 1004);

        let conditions: Vec<QueryConditionType> =
            vec![QueryConditionType::EQ, QueryConditionType::GTE];
        let keys_vec: Vec<u64> = vec![2, 1002];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            2,
            0,
            0,
        );
        assert_eq!(result.len(), 3);
        assert_eq!(*result.get(0).unwrap(), 1002);
        assert_eq!(*result.get(1).unwrap(), 1003);
        assert_eq!(*result.get(2).unwrap(), 1004);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_idx_3_layer_cond() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));
        columns.push(column_descr!(0x2, 0x1, 0, &INT, "pk3".as_bytes().to_vec()));

        let mut pk_map = CompositePkOAHashMapIdx::new(&idx_name, 1024, &columns).unwrap();

        let mut pk_idx: Box<dyn PkIdx + Sync + Send> = Box::new(pk_map);

        let col_types: Vec<&DataType> = vec![&LONG, &INT, &INT];

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 20..25 {
                let mut third_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
                for t in 30..35 {
                    let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                    let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                    let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(t as u32).to_vec());
                    let pk = PrimaryKey {
                        primary: col,
                        secondary: Option::Some(vec![col2, col3]),
                    };
                    pk_idx.put(&pk, t);
                    let row = Row {
                        pk: pk,
                        timestamp: 0,
                        tx_id: 1,
                        payload: vec![],
                        deleted: false,
                    };
                    third_layer.insert(
                        t,
                        RangeIndexKey {
                            primary: t,
                            from: 0,
                            to: 0,
                            secondary_keys: Option::None,
                            value: Option::Some(Arc::new(row)),
                        },
                    );
                }

                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 30,
                        to: 35,
                        secondary_keys: Option::Some(third_layer),
                        value: Option::None,
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 20,
                    to: 25,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col_byte_sizes: Vec<u8> = col_types.iter().map(|c| c.bytes_size).collect();
        let file_size = calculate_size(&first_layer, &col_byte_sizes) + RANGE_IDX_DATA_OFFSET;
        let mut file = MmapStruct::new_limited(&row_name, file_size as u64 * 10).unwrap();

        store_segment_keys(
            &mut file,
            &first_layer,
            &pk_idx,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
        );

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::LT,
        ];
        let keys_vec: Vec<u64> = vec![2, 20, 33];
        let result = find_range(
            &file,
            &keys_vec,
            &conditions,
            &col_types,
            RANGE_IDX_DATA_OFFSET,
            0,
            3,
            0,
            0,
        );
        assert_eq!(result.len(), 3);

        fs::remove_file(&row_name);
        fs::remove_file(&idx_name);
    }

    #[test]
    fn test_find_range_mem_idx_3_layer_eq() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));
        columns.push(column_descr!(0x2, 0x1, 0, &INT, "pk3".as_bytes().to_vec()));

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 20..25 {
                let mut third_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
                for t in 30..35 {
                    let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                    let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                    let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(t as u32).to_vec());
                    let pk = PrimaryKey {
                        primary: col,
                        secondary: Option::Some(vec![col2, col3]),
                    };
                    let row = Row {
                        pk: pk,
                        timestamp: 0,
                        tx_id: 1,
                        payload: vec![],
                        deleted: false,
                    };
                    third_layer.insert(
                        t,
                        RangeIndexKey {
                            primary: t,
                            from: 0,
                            to: 0,
                            secondary_keys: Option::None,
                            value: Option::Some(Arc::new(row)),
                        },
                    );
                }

                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 30,
                        to: 35,
                        secondary_keys: Option::Some(third_layer),
                        value: Option::None,
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 20,
                    to: 25,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(1).to_vec());
        let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(20 as u32).to_vec());
        let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(30 as u32).to_vec());
        let res = range_idx_select_eq(&first_layer, &vec![&col, &col2, &col3], 0);

        assert!(res.is_some());
    }

    #[test]
    fn test_find_range_mem_idx_3_layer_conditions() {
        let idx_name = format!("{}{}", IDX_PATH, getrandom::u64().unwrap());
        let row_name = format!("{}{}", RAW_FILE_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk1".as_bytes().to_vec()));
        columns.push(column_descr!(0x1, 0x1, 0, &INT, "pk2".as_bytes().to_vec()));
        columns.push(column_descr!(0x2, 0x1, 0, &INT, "pk3".as_bytes().to_vec()));

        let mut first_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
        for i in 0..10 {
            let mut second_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
            for s in 20..25 {
                let mut third_layer: BTreeMap<u64, RangeIndexKey> = BTreeMap::new();
                for t in 30..35 {
                    let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(i).to_vec());
                    let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(s as u32).to_vec());
                    let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(t as u32).to_vec());
                    let pk = PrimaryKey {
                        primary: col,
                        secondary: Option::Some(vec![col2, col3]),
                    };
                    let row = Row {
                        pk: pk,
                        timestamp: 0,
                        tx_id: 1,
                        payload: vec![],
                        deleted: false,
                    };
                    third_layer.insert(
                        t,
                        RangeIndexKey {
                            primary: t,
                            from: 0,
                            to: 0,
                            secondary_keys: Option::None,
                            value: Option::Some(Arc::new(row)),
                        },
                    );
                }

                second_layer.insert(
                    s,
                    RangeIndexKey {
                        primary: s,
                        from: 30,
                        to: 35,
                        secondary_keys: Option::Some(third_layer),
                        value: Option::None,
                    },
                );
            }

            first_layer.insert(
                i,
                RangeIndexKey {
                    primary: i,
                    from: 20,
                    to: 25,
                    secondary_keys: Option::Some(second_layer),
                    value: Option::None,
                },
            );
        }

        let col = new_column!(0, LONG.id, 0, 0x1, u64::to_le_bytes(1).to_vec());
        let col2 = new_column!(1, INT.id, 0, 0x1, u32::to_le_bytes(20 as u32).to_vec());
        let col3 = new_column!(2, INT.id, 0, 0x1, u32::to_le_bytes(20 as u32).to_vec());

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::GT,
            QueryConditionType::GT,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 12);

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::LTE,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 2);
        assert_eq!(res.as_ref().unwrap().get(1).unwrap().pk.primary.num_val, 1);
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .num_val,
            20
        );
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .num_val,
            31
        );

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::LT,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 1);
        assert_eq!(res.as_ref().unwrap().get(0).unwrap().pk.primary.num_val, 1);
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .num_val,
            20
        );
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .num_val,
            30
        );

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::GT,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 3);
        assert_eq!(res.as_ref().unwrap().get(0).unwrap().pk.primary.num_val, 1);
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .num_val,
            20
        );
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(2)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .num_val,
            34
        );

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::GTE,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 4);
        assert_eq!(res.as_ref().unwrap().get(0).unwrap().pk.primary.num_val, 1);
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(0)
                .unwrap()
                .num_val,
            20
        );
        assert_eq!(
            res.as_ref()
                .unwrap()
                .get(2)
                .unwrap()
                .pk
                .secondary
                .as_ref()
                .unwrap()
                .get(1)
                .unwrap()
                .num_val,
            33
        );

        let conditions: Vec<QueryConditionType> = vec![
            QueryConditionType::EQ,
            QueryConditionType::EQ,
            QueryConditionType::EQ,
        ];
        let res = range_idx_select_conditions(&first_layer, &vec![1, 20, 31], &conditions, 0);
        assert!(res.is_some());
        assert_eq!(res.as_ref().unwrap().len(), 1);
        assert_eq!(res.unwrap().get(0).unwrap().pk.primary.num_val, 1);
    }
}
