// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use anyhow::ensure;
use std::fs::File;
use std::io::{self, Seek, Write};
use std::sync::atomic::{AtomicU32, AtomicU64};

use crate::model::Column;
use crate::model::data_type::DataType;
use crate::{data::mem_mapped_struct::MmapStruct, model::data_type::map_data_type};

use anyhow::Result;

/// Main DB entities for data persistence
/// Directory structure for example table
/// create keysapce ks1
/// create table ks1.long_vectors (pk long, vec long_vector[8],uud varchar, primary key (pk))
/// /db_root/ks1-1212/keyspace_meta.db -- 1212 is keyspace ID -- KeyspaceStruct
/// /db_root/ks1/long_vectors-400202/ -- table dir, 400202 - table id
/// /db_root/ks1/long_vectors-400202/table_meta.db TableStruct
/// /db_root/ks1/long_vectors-400202/long_vectors_range_idx_meta.db dx_meta.db IdxMetaStruct in case of create table ... with (range_idx=true)
///
/// every memtable flushed as segment with new segment id
/// all segment files have segment id prefix
/// segment id 1029
/// /db_root/ks1/long_vectors-400202/1029_segment_meta.db -- segment meta data SegmentMetaStruct
/// /db_root/ks1/long_vectors-400202/1029.pk_idx -- primary key hash map U64OAHashMapIdx
/// /db_root/ks1/long_vectors-400202/1029.composite_pk_idx -- primary key hash map CompositePkOAHashMapIdx in case of composite pk
/// /db_root/ks1/long_vectors-400202/1029_pk.flat -- pk column values FlatValStruct
/// /db_root/ks1/long_vectors-400202/1029_vec.vec -- vec column values VectorStruct
/// /db_root/ks1/long_vectors-400202/1029_uud.var -- uud column values VarLenStruct
/// /db_root/ks1/long_vectors-400202/1029_long_vectors.range_idx RangeIdxStruct in case of create table ... with (range_idx=true)
/// /db_root/ks1/long_vectors-400202/1029_segment_summary.db -- SegmentSummaryStruct segment summary, writes after all data files persisted. if not exists - segment not valid

pub static NAME_LENGTH_LIMIT: usize = 15;

pub static ENTITY_HEADER_LENGTH: usize = 32;
pub static SUMMARY_HEADER_LENGTH: usize = 20;
pub static ENTITY_NAME_OFFSET: usize = 64;
pub static META_HEADER_LENGTH: usize = 64;
pub static COLUMN_DESCR_OFFSET: usize = 128;
pub static COLUMN_SEGMENT_HEADER_LENGTH: usize = 64;

pub static CRC_OFFSET: usize = 28;
pub static ROW_META_LENGTH: u64 = 32;

pub static BASE_ENTITY_LENGTH: u64 = 8192;

pub const MAGIC: u64 = u64::from_le_bytes(*b"MRDB0001");

pub trait FromToMmap {
    fn save_to_mapped_file(&self, file: &mut MmapStruct, offset: usize) -> usize;
    fn save_to_raw_file(&self, file: &mut File) -> usize;
    //panic in case of less file size
    fn read_from_mapped_file(file: &MmapStruct, offset: usize) -> Self;
}

#[macro_export]
macro_rules! e_header {
    ($id:expr, $magic:expr, $storage_version:expr, $created:expr) => {
        EntityHeader {
            created: $created,
            magic: $magic,
            storage_version: $storage_version,
            id: $id,
        }
    };
}

#[derive(Debug, Clone)]
pub struct EntityHeader {
    pub magic: u64,
    pub created: u64,
    pub storage_version: u32,
    pub id: u32,
}

impl FromToMmap for EntityHeader {
    fn save_to_mapped_file(&self, file: &mut MmapStruct, offset: usize) -> usize {
        file.put_u64(offset, self.magic);
        file.put_u64(offset + 8, self.created);
        file.put_u32(offset + 16, self.storage_version);
        file.put_u32(offset + 20, self.id);
        ENTITY_HEADER_LENGTH
    }

    fn read_from_mapped_file(file: &MmapStruct, offset: usize) -> Self {
        let magic = file.get_u64(offset);
        let created = file.get_u64(offset + 8);
        let storage_version = file.get_u32(offset + 16);
        let id = file.get_u32(offset + 20);
        EntityHeader {
            created,
            magic,
            storage_version: storage_version,
            id,
        }
    }

    fn save_to_raw_file(&self, file: &mut File) -> usize {
        file.write(&u64::to_le_bytes(self.magic));
        file.write(&u64::to_le_bytes(self.created));
        file.write(&u32::to_le_bytes(self.storage_version));
        file.write(&u32::to_le_bytes(self.id));
        ENTITY_HEADER_LENGTH
    }
}

pub static COLUMN_DESCR_LENGTH: usize = 64;

#[derive(Debug, Clone)]
pub struct ColumnDescription {
    pub dimension: u16,
    pub data_type: &'static DataType,
    pub col_id: u8,
    pub flags: u8,
    pub name: Vec<u8>, //utf-8
}

impl FromToMmap for ColumnDescription {
    fn save_to_mapped_file(&self, file: &mut MmapStruct, offset: usize) -> usize {
        if self.name.len() > NAME_LENGTH_LIMIT {
            panic!("entity name length sholuld be < 15 bytes")
        }

        file.put_u16(offset, self.dimension);
        file.put_u8(offset + 2, self.col_id);
        file.put_u8(offset + 3, self.data_type.id);
        file.put_u8(offset + 4, self.flags);
        file.put_string_with_size(offset + 5, &self.name);

        COLUMN_DESCR_LENGTH
    }

    fn read_from_mapped_file(file: &MmapStruct, offset: usize) -> Self {
        let dimension = file.get_u16(offset);
        let col_id = file.get_u8(offset + 2);
        let data_type = file.get_u8(offset + 3);
        let flags = file.get_u8(offset + 4);
        let name = file.get_sized_string(offset + 5).1.to_vec();
        Self {
            dimension,
            col_id,
            data_type: map_data_type(data_type),
            flags,
            name,
        }
    }

    fn save_to_raw_file(&self, file: &mut File) -> usize {
        if self.name.len() > NAME_LENGTH_LIMIT {
            panic!("entity name length sholuld be < 15 bytes")
        }

        file.write(&u16::to_le_bytes(self.dimension));
        file.write(&u8::to_le_bytes(self.col_id));
        file.write(&u8::to_le_bytes(self.data_type.id));
        file.write(&u8::to_le_bytes(self.flags));

        let name_len = self.name.len() as u8;
        file.write(&u8::to_le_bytes(name_len));
        file.write_all(&self.name);

        COLUMN_DESCR_LENGTH
    }
}

fn write_column_desription(file: &mut MmapStruct, d: &ColumnDescription, offset: usize) -> usize {
    let mut offset: usize = offset;
    file.put_u16(offset, d.dimension);
    offset += 2;
    file.put_u8(offset, d.data_type.id);
    offset += 1;
    file.put_u8(offset, d.flags);
    offset += 1;
    file.put_u8(offset, d.col_id);
    offset += 1;
    file.put_string_with_size(offset, &d.name);

    COLUMN_DESCR_LENGTH
}

fn read_column_desription(
    file: &MmapStruct,
    offset: usize,
) -> (u16, &'static DataType, u8, u8, Vec<u8>) {
    let mut offset: usize = offset;
    let dimension = file.get_u16(offset);
    offset += 2;
    let data_type = file.get_u8(offset);
    offset += 1;
    let flags = file.get_u8(offset);
    offset += 1;
    let col_id = file.get_u8(offset);
    offset += 1;

    let name = file.get_sized_string(offset).1.to_vec();

    (dimension, map_data_type(data_type), flags, col_id, name)
}

#[derive(Debug, Clone)]
pub struct KeyspaceStruct {
    pub root: String,
    pub header: EntityHeader,
    pub name: String,
}

impl KeyspaceStruct {
    pub fn new(
        directory: &String,
        meta_path: &String,
        header: &EntityHeader,
        name: &String,
    ) -> Result<Self> {
        let mut file = MmapStruct::new_limited(meta_path, BASE_ENTITY_LENGTH)?;
        header.save_to_mapped_file(&mut file, 0);
        file.put_string_with_size(ENTITY_NAME_OFFSET, name.as_bytes());
        file.close()?;

        Ok(Self {
            root: directory.clone(),
            header: header.clone(),
            name: name.clone(),
        })
    }

    pub fn load(root: &String, meta_path: &String) -> Result<Self> {
        let file = MmapStruct::load(meta_path)?;
        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let name = String::from_utf8(file.get_sized_string(ENTITY_NAME_OFFSET).1.to_vec())?;
        Ok(Self {
            root: root.clone(),
            header,
            name,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableStruct {
    pub root: String,
    pub header: EntityHeader,
    pub flags: u32,
    pub name: String,
    pub columns: Vec<ColumnDescription>,
    pub pk_size: u8,
}

impl TableStruct {
    pub fn new(
        root: &String,
        meta_path: &String,
        header: &EntityHeader,
        name: &String,
        flags: u32,
        columns: &Vec<ColumnDescription>,
        pk_size: u8,
    ) -> Result<Self> {
        let mut file = MmapStruct::new_limited(meta_path, BASE_ENTITY_LENGTH)?;
        header.save_to_mapped_file(&mut file, 0);
        file.put_string_with_size(ENTITY_NAME_OFFSET, name.as_bytes());

        let offset = ENTITY_HEADER_LENGTH;
        file.put_u32(offset, flags);
        file.put_u8(offset + 4, columns.len() as u8);
        file.put_u8(offset + 5, pk_size);

        write_columns(&mut file, columns, COLUMN_DESCR_OFFSET);

        put_crc(&mut file);
        file.close()?;

        Ok(Self {
            root: root.clone(),
            header: header.clone(),
            flags,
            name: name.clone(),
            pk_size,
            columns: columns.clone(),
        })
    }

    pub fn load(root: &String, meta_path: &String) -> Result<Self> {
        let file = MmapStruct::load(meta_path)?;

        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let name = String::from_utf8(file.get_sized_string(ENTITY_NAME_OFFSET).1.to_vec())?;

        let offset = ENTITY_HEADER_LENGTH;
        let flags = file.get_u32(offset);
        let columns_count = file.get_u8(offset + 4);
        let pk_size = file.get_u8(offset + 5);

        let mut columns: Vec<ColumnDescription> =
            read_columns(&file, columns_count, COLUMN_DESCR_OFFSET);

        Ok(Self {
            root: root.clone(),
            header,
            flags,
            name,
            pk_size,
            columns,
        })
    }
}

pub fn put_crc(file: &mut MmapStruct) {
    let crc = file.calculate_crc(ENTITY_HEADER_LENGTH);
    file.put_u32(CRC_OFFSET, crc);
}

pub fn check_crc(file: &MmapStruct) -> Result<()> {
    let crc = file.get_u32(CRC_OFFSET);
    let crc_calculated = file.calculate_crc(ENTITY_HEADER_LENGTH);
    ensure!(
        crc == crc_calculated,
        "crc invalid for {}; remove all segment files and restart DB",
        file.path
    );
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RowDataOffset {
    pub file_offset: u32,
    pub col_id: u8,
}

#[derive(Debug, Clone)]
pub struct VarLengthRowData {
    pub file_offset: u32,
    pub data_type: u8,
    pub col_id: u8,
}

#[derive(Debug, Clone)]
pub struct RowMeta {
    pub tx_id: u64,
    pub created: u64,
    pub idx: u64,
    pub not_null_cols: u64,
    pub var_len_rows: Vec<RowDataOffset>,
}

pub struct SegmentMetaStruct {
    pub path: String,
    file: Option<File>,
    mapped_file: Option<MmapStruct>,
    pub header: EntityHeader,
    pub columns: Vec<ColumnDescription>,
    pub col_var_len_mask: u64,
    pub col_mask: u64,
    pub rows_count: u32,
    pub offset: AtomicU64,
}

impl SegmentMetaStruct {
    pub fn new(
        path: &String,
        header: &EntityHeader,
        rows_count: u32,
        columns: &Vec<ColumnDescription>,
    ) -> Result<Self> {
        let var_len_columns_count =
            columns.iter().filter(|&c| c.data_type.var_length).count() as u64;

        let file_size =
            META_HEADER_LENGTH as u64 + rows_count as u64 * 32 + 6 * var_len_columns_count;

        let mut file = File::create(&path)?;
        file.set_len(file_size as u64);
        file.seek(io::SeekFrom::Start((0)));

        header.save_to_raw_file(&mut file);

        let mut columns_mask: u64 = 0;
        let mut columns_var_len_mask: u64 = 0;

        let mut offset = ENTITY_HEADER_LENGTH as u64;
        file.seek(io::SeekFrom::Start((offset)));

        for i in 0..columns.len() {
            let d = columns.get(i).unwrap();
            columns_mask |= 0x1 << d.col_id;
            if (d.data_type.var_length) {
                columns_var_len_mask |= 0x1 << d.col_id;
            }
        }

        file.write(&u64::to_le_bytes(columns_mask));
        file.write(&u64::to_le_bytes(columns_var_len_mask));
        file.write(&u32::to_le_bytes(rows_count));

        file.seek(io::SeekFrom::Start(COLUMN_SEGMENT_HEADER_LENGTH as u64));

        Ok(Self {
            path: path.clone(),
            file: Option::Some(file),
            mapped_file: Option::None,
            header: header.clone(),
            columns: columns.clone(),
            rows_count: rows_count,
            col_mask: columns_mask,
            col_var_len_mask: columns_var_len_mask,
            offset: AtomicU64::new(COLUMN_SEGMENT_HEADER_LENGTH as u64),
        })
    }

    pub fn load(path: &String, columns: &Vec<ColumnDescription>) -> Result<Self> {
        let file = MmapStruct::load(path)?;

        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let columns_mask = file.get_u64(ENTITY_HEADER_LENGTH);
        let columns_var_len_mask = file.get_u64(ENTITY_HEADER_LENGTH + 8);
        let rows_count = file.get_u32(ENTITY_HEADER_LENGTH + 16);

        Ok(Self {
            path: path.clone(),
            file: Option::None,
            header,
            mapped_file: Option::Some(file),
            columns: columns.clone(),
            col_var_len_mask: columns_var_len_mask,
            col_mask: columns_mask,
            rows_count: rows_count,
            offset: AtomicU64::new(META_HEADER_LENGTH as u64),
        })
    }

    fn write_row_meta(
        file: &mut File,
        idx: u64,
        tx_id: u64,
        created: u64,
        not_null_cols: u64,
        var_len_data: &Vec<VarLengthRowData>,
    ) -> u32 {
        file.write(&u64::to_le_bytes(idx));
        file.write(&u64::to_le_bytes(tx_id));
        file.write(&u64::to_le_bytes(created));
        file.write(&u64::to_le_bytes(not_null_cols));

        let mut row_length: u32 = 32;

        if not_null_cols > 0 {
            for (i, r) in var_len_data.iter().enumerate() {
                if (map_data_type(r.data_type).var_length) {
                    file.write(&u32::to_le_bytes(r.file_offset));
                    file.write(&u8::to_le_bytes(r.col_id));
                    file.write(&u8::to_le_bytes(r.data_type));
                    row_length += 6;
                }
            }
        }

        row_length
    }

    pub fn insert_row_meta(
        &mut self,
        idx: u64,
        tx_id: u64,
        created: u64,
        deleted: bool,
        columns: &Vec<Column>,
        var_len_data: &Vec<VarLengthRowData>,
    ) -> u64 {
        let base_offset = self.offset.load(std::sync::atomic::Ordering::Acquire);

        let mut row_length: u32 = 0;
        let mut file = self.file.as_mut().unwrap();
        let mut non_null_columns: u64 = 0; // pk alway have 0 index. if pk null -> column were deleted (i.e. tombstone)

        if (!deleted) {
            for (idx, c) in columns.iter().enumerate() {
                non_null_columns |= 0x1 << c.col_id;
            }
        }

        let length = Self::write_row_meta(
            &mut file,
            idx,
            tx_id,
            created,
            non_null_columns,
            var_len_data,
        );
        row_length += length;

        let next_offset = base_offset + row_length as u64;
        self.offset
            .store(next_offset, std::sync::atomic::Ordering::Release);

        base_offset
    }

    pub fn get_row_meta(&self, row_offset: u64) -> RowMeta {
        let file = self.mapped_file.as_ref().unwrap();
        let mut offset = row_offset as usize;
        let idx = file.get_u64(offset);
        offset += 8;
        let tx_id = file.get_u64(offset);
        offset += 8;
        let created = file.get_u64(offset);
        offset += 8;
        let not_null_cols: u64 = file.get_u64(offset);
        offset += 8;

        let mut var_len_rows: Vec<RowDataOffset> = vec![];

        if not_null_cols > 0 {
            let data_offsets_count = u64::count_ones(not_null_cols & self.col_var_len_mask);
            for i in 0..data_offsets_count {
                let file_offset = file.get_u32(offset);
                offset += 4;
                let col_id = file.get_u8(offset);
                offset += 1;
                let data_type = file.get_u8(offset);
                offset += 1;
                var_len_rows.push(RowDataOffset {
                    file_offset,
                    col_id,
                });
            }
        }
        RowMeta {
            tx_id,
            created,
            idx,
            not_null_cols,
            var_len_rows,
        }
    }

    pub fn flush_and_remap(&mut self) -> Result<()> {
        match self.file.as_mut() {
            Some(mut f) => {
                f.sync_all()?;
                drop(f);
                self.file = Option::None;

                let mut file = MmapStruct::load(&self.path)?;
                put_crc(&mut file);
                file.flush()?;
                self.mapped_file = Option::Some(file);
            }
            None => {}
        }
        Ok(())
    }
}

impl Drop for SegmentMetaStruct {
    fn drop(&mut self) {
        println!("drop meta");
    }
}

#[derive(Debug, Clone)]
pub struct PkRange {
    pub pk_num_from: u64,
    pub pk_num_to: u64,
    pub pk_bytes_from: Vec<u8>,
    pub pk_bytes_to: Vec<u8>,
    pub pk_composite_from: Vec<u64>,
    pub pk_composite_to: Vec<u64>,
    pub single_num: bool,
}

fn store_pk_range(file: &mut MmapStruct, pk_range: &PkRange, offset: usize) -> usize {
    let mut offset = offset;

    let from_length = pk_range.pk_bytes_from.len();
    let to_length = pk_range.pk_bytes_to.len();

    file.put_u16(offset, from_length as u16);
    offset += 2;
    file.mmap[offset..offset + from_length].copy_from_slice(&pk_range.pk_bytes_from);
    offset += from_length;

    file.put_u16(offset, to_length as u16);
    offset += 2;
    file.mmap[offset..offset + to_length].copy_from_slice(&pk_range.pk_bytes_to);
    offset += to_length;

    file.put_u16(offset, pk_range.pk_composite_from.len() as u16);
    offset += 2;
    file.put_u64_vector(offset, &pk_range.pk_composite_from);
    offset += pk_range.pk_composite_from.len() * 8;

    file.put_u16(offset, pk_range.pk_composite_to.len() as u16);
    offset += 2;
    file.put_u64_vector(offset, &pk_range.pk_composite_to);
    offset += 8 * pk_range.pk_composite_to.len();
    offset - PK_RANGE_OFFSET
}

pub fn read_pk_range(file: &mut MmapStruct, offset: usize) -> PkRange {
    let mut offset = offset;

    let from_length = file.get_u16(offset) as usize;
    offset += 2;
    let pk_bytes_from = &file.mmap[offset..offset + from_length].to_vec();
    offset += from_length;

    let to_length = file.get_u16(offset) as usize;
    offset += 2;
    let pk_bytes_to = &file.mmap[offset..offset + to_length].to_vec();
    offset += to_length;

    let composite_from_len = file.get_u16(offset) as usize;
    offset += 2;
    let pk_composite_from = file.get_u64_vector(offset, composite_from_len as u16);
    offset += composite_from_len * 8;
    let composite_to_len = file.get_u16(offset) as usize;
    offset += 2;
    let pk_composite_to = file.get_u64_vector(offset, composite_to_len as u16);

    PkRange {
        single_num: pk_composite_from.len() == 1,
        pk_num_from: *pk_composite_from
            .get(0)
            .expect("segment summary should contain pk range"),
        pk_num_to: *pk_composite_to
            .get(0)
            .expect("segment summary should contain pk range"),
        pk_bytes_from: pk_bytes_from.to_vec(),
        pk_bytes_to: pk_bytes_to.to_vec(),
        pk_composite_from: pk_composite_from,
        pk_composite_to: pk_composite_to,
    }
}

pub struct SegmentSummaryStruct {
    pub path: String,
    pub header: EntityHeader,
    pub tx_id_min: u64,
    pub tx_id_max: u64,
    pub columns: Vec<ColumnDescription>,
    pub rows_count: u32,
    pub pk_range: PkRange,
}

static PK_RANGE_OFFSET: usize = 4096;

impl SegmentSummaryStruct {
    pub fn new(
        path: &String,
        header: &EntityHeader,
        tx_id_min: u64,
        tx_id_max: u64,
        rows_count: u32,
        columns: &Vec<ColumnDescription>,
        pk_range: PkRange,
    ) -> Result<Self> {
        let mut file = MmapStruct::new_limited(path, BASE_ENTITY_LENGTH)?;
        header.save_to_mapped_file(&mut file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        file.put_u64(offset, tx_id_min);
        file.put_u64(offset + 8, tx_id_max);
        file.put_u32(offset + 16, rows_count);
        file.put_u8(offset + 20, columns.len() as u8);

        write_columns(&mut file, columns, COLUMN_DESCR_OFFSET);
        store_pk_range(&mut file, &pk_range, PK_RANGE_OFFSET);

        put_crc(&mut file);

        println!("flush summary {}", path);
        file.flush()?;

        Ok(Self {
            path: path.clone(),
            header: header.clone(),
            tx_id_min: tx_id_min,
            tx_id_max: tx_id_max,
            columns: columns.clone(),
            rows_count: rows_count,
            pk_range,
        })
    }

    pub fn load(path: &String) -> Result<Self> {
        let mut file = MmapStruct::load(path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        let tx_id_min = file.get_u64(offset);
        let tx_id_max = file.get_u64(offset + 8);
        let rows_count = file.get_u32(offset + 16);
        let columns_count = file.get_u8(offset + 20);

        let mut columns: Vec<ColumnDescription> =
            read_columns(&file, columns_count, COLUMN_DESCR_OFFSET);

        let pk_range = read_pk_range(&mut file, PK_RANGE_OFFSET);

        Ok(Self {
            path: path.clone(),
            header: header,
            tx_id_min,
            tx_id_max,
            columns,
            rows_count,
            pk_range,
        })
    }
}

fn read_columns(file: &MmapStruct, columns_count: u8, offset: usize) -> Vec<ColumnDescription> {
    let mut offset = offset;
    let mut columns: Vec<ColumnDescription> = vec![];
    for i in 0..columns_count {
        columns.push(ColumnDescription::read_from_mapped_file(file, offset));
        offset += COLUMN_DESCR_LENGTH;
    }
    columns
}

fn write_columns(file: &mut MmapStruct, columns: &Vec<ColumnDescription>, offset: usize) {
    let mut offset = offset;
    for c in columns {
        c.save_to_mapped_file(file, offset);
        offset += COLUMN_DESCR_LENGTH;
    }
}

pub struct FlatValStruct {
    pub path: String,
    pub header: EntityHeader,
    pub data_type: DataType,
    pub dimension: u16,
    pub row_limit: u32,
    pub col_id: u8,
    file: MmapStruct,
}

impl FlatValStruct {
    pub fn new(
        file_path: &String,
        header: &EntityHeader,
        data_type: DataType,
        dimension: u16,
        col_id: u8,
        row_limit: u32,
    ) -> Result<Self> {
        let mut file_size = if data_type.var_length {
            row_limit * dimension as u32
        } else {
            row_limit * data_type.bytes_size as u32
        };

        file_size += COLUMN_SEGMENT_HEADER_LENGTH as u32;

        println!("flat size {}", file_size);

        let mut file = MmapStruct::new_limited(file_path, file_size as u64)?;
        header.save_to_mapped_file(&mut file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        file.put_u16(offset, dimension);
        file.put_u8(offset + 2, data_type.id);
        file.put_u8(offset + 3, col_id);

        Ok(FlatValStruct {
            path: file_path.clone(),
            header: header.clone(),
            data_type,
            dimension,
            col_id,
            row_limit,
            file,
        })
    }

    pub fn load(file_path: &String) -> Result<Self> {
        let file = MmapStruct::load(file_path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let file_size = file.file_size - COLUMN_SEGMENT_HEADER_LENGTH as u64;

        let offset = ENTITY_HEADER_LENGTH;
        let dimension = file.get_u16(offset);
        let data_type = map_data_type(file.get_u8(offset + 2)).clone();
        let col_id = file.get_u8(offset + 3);

        let row_limit = if data_type.var_length {
            file_size / dimension as u64
        } else {
            file_size / data_type.bytes_size as u64
        };

        Ok(FlatValStruct {
            path: file_path.clone(),
            header: header,
            data_type,
            dimension,
            col_id,
            row_limit: row_limit as u32,
            file,
        })
    }

    pub fn put_bytes_val(&mut self, idx: u32, bytes: &Vec<u8>) {
        unsafe {
            let record_length = if self.data_type.var_length {
                self.dimension
            } else {
                self.data_type.bytes_size as u16
            };
            let offset = COLUMN_SEGMENT_HEADER_LENGTH + record_length as usize * idx as usize;
            self.file.put_bytes(offset, &bytes);
        }
    }

    pub fn get_bytes_val(&self, idx: u32) -> Vec<u8> {
        let record_length = if self.data_type.var_length {
            self.dimension
        } else {
            self.data_type.bytes_size as u16
        };
        let offset = COLUMN_SEGMENT_HEADER_LENGTH + record_length as usize * idx as usize;
        self.file.get_bytes(offset, record_length as usize)
    }

    pub fn copy_bytes_val(&self, data: &mut Vec<u8>, idx: usize, to_offset: usize) -> usize {
        let record_length = if self.data_type.var_length {
            self.dimension as usize
        } else {
            self.data_type.bytes_size as usize
        };
        let from_offset = COLUMN_SEGMENT_HEADER_LENGTH + record_length as usize * idx as usize;
        self.file
            .copy_bytes(data, from_offset, to_offset, record_length);
        record_length
    }

    pub fn flush(&mut self) -> Result<()> {
        put_crc(&mut self.file);
        self.file.flush()?;
        Ok(())
    }
}

impl Drop for FlatValStruct {
    fn drop(&mut self) {
        //println!("drop flat");
    }
}

pub struct VectorStruct {
    pub path: String,
    pub header: EntityHeader,
    data_type: DataType,
    dimension: u16,
    col_id: u8,
    row_limit: u32,
    row_size: u32,
    file: MmapStruct,
}

impl VectorStruct {
    pub fn new(
        file_path: &String,
        header: &EntityHeader,
        data_type: DataType,
        dimension: u16,
        col_id: u8,
        row_limit: u32,
    ) -> Result<Self> {
        let file_size = row_limit * dimension as u32 * data_type.bytes_size as u32
            + COLUMN_SEGMENT_HEADER_LENGTH as u32;

        let mut file = MmapStruct::new_limited(file_path, file_size as u64)?;
        header.save_to_mapped_file(&mut file, 0);

        let offset = ENTITY_HEADER_LENGTH;
        file.put_u16(offset, dimension);
        file.put_u8(offset + 2, data_type.id);
        file.put_u8(offset + 3, col_id);

        put_crc(&mut file);

        Ok(VectorStruct {
            path: file_path.clone(),
            header: header.clone(),
            data_type,
            dimension,
            col_id,
            row_limit,
            row_size: dimension as u32 * data_type.bytes_size as u32,
            file,
        })
    }

    pub fn load(file_path: &String) -> Result<Self> {
        let mut file = MmapStruct::load(file_path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&mut file, 0);
        let offset = ENTITY_HEADER_LENGTH;
        let dimension = file.get_u16(offset);
        let data_type = map_data_type(file.get_u8(offset + 2)).clone();
        let col_id = file.get_u8(offset + 3);

        let row_size = dimension as u32 * data_type.bytes_size as u32;
        Ok(VectorStruct {
            path: file_path.clone(),
            header,
            data_type,
            dimension,
            col_id,
            row_limit: (file.file_size as u32 - COLUMN_SEGMENT_HEADER_LENGTH as u32) / row_size,
            row_size,
            file,
        })
    }

    pub fn put_vector_bytes(&mut self, idx: u32, bytes: &Vec<u8>) {
        let offset = idx as usize * self.dimension as usize * self.data_type.bytes_size as usize
            + COLUMN_SEGMENT_HEADER_LENGTH;
        unsafe {
            self.file.put_bytes(offset, bytes);
        }
    }

    pub fn get_vector_bytes(&self, idx: usize) -> Vec<u8> {
        let len = self.dimension as usize * self.data_type.bytes_size as usize;
        let start = idx * self.row_size as usize + COLUMN_SEGMENT_HEADER_LENGTH;
        self.file.get_bytes(start, len)
    }

    pub fn copy_vector_bytes(&self, data: &mut Vec<u8>, idx: usize, to_offset: usize) -> usize {
        let len = self.dimension as usize * self.data_type.bytes_size as usize;
        let start = idx * self.row_size as usize + COLUMN_SEGMENT_HEADER_LENGTH;
        self.file.copy_bytes(data, start, to_offset, len);
        len
    }

    pub fn put_u8_vector(&mut self, offset: usize, vec: Vec<u8>) {
        self.file
            .put_u8_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn put_u16_vector(&mut self, offset: usize, vec: &Vec<u16>) {
        self.file
            .put_u16_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn put_u32_vector(&mut self, offset: usize, vec: &Vec<u32>) {
        self.file
            .put_u32_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn put_u64_vector(&mut self, offset: usize, vec: &Vec<u64>) {
        self.file
            .put_u64_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn put_f32_vector(&mut self, offset: usize, vec: &Vec<f32>) {
        self.file
            .put_f32_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn put_f64_vector(&mut self, offset: usize, vec: &Vec<f64>) {
        self.file
            .put_f64_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, vec);
    }

    pub fn get_u8_vector(&self, offset: usize) -> Vec<u8> {
        self.file
            .get_u8_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn get_u16_vector(&self, offset: usize) -> Vec<u16> {
        self.file
            .get_u16_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn get_u32_vector(&self, offset: usize) -> Vec<u32> {
        self.file
            .get_u32_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn get_u64_vector(&self, offset: usize) -> Vec<u64> {
        self.file
            .get_u64_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn get_f32_vector(&self, offset: usize) -> Vec<f32> {
        self.file
            .get_f32_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn get_f64_vector(&self, offset: usize) -> Vec<f64> {
        self.file
            .get_f64_vector(offset + COLUMN_SEGMENT_HEADER_LENGTH, self.dimension)
    }

    pub fn flush(&mut self) -> Result<()> {
        put_crc(&mut self.file);
        self.file.flush()?;
        Ok(())
    }
}

impl Drop for VectorStruct {
    fn drop(&mut self) {
        //println!("drop varl");
    }
}

pub struct VarLenStruct {
    pub path: String,
    pub header: EntityHeader,
    pub data_type: DataType,
    pub col_id: u8,
    initial_size: u64,
    file_size: AtomicU32,
    pub file: Option<File>,
    pub mapped_file: Option<MmapStruct>,
    offset: AtomicU32,
}

impl VarLenStruct {
    pub fn new(
        file_path: &String,
        header: &EntityHeader,
        data_type: DataType,
        col_id: u8,
        initial_size: u64,
    ) -> Result<Self> {
        let mut file = File::create(file_path.clone())?;
        file.set_len(initial_size)?;
        file.seek(io::SeekFrom::Start(0))?;

        header.save_to_raw_file(&mut file);
        file.seek(io::SeekFrom::Start(ENTITY_HEADER_LENGTH as u64))?;
        file.write(&u8::to_le_bytes(data_type.id))?;
        file.write(&u8::to_le_bytes(col_id))?;

        file.seek(io::SeekFrom::Start(COLUMN_SEGMENT_HEADER_LENGTH as u64))?;

        Ok(VarLenStruct {
            path: file_path.clone(),
            header: header.clone(),
            data_type,
            initial_size,
            col_id,
            file: Option::Some(file),
            file_size: AtomicU32::new(initial_size as u32),
            offset: AtomicU32::new(COLUMN_SEGMENT_HEADER_LENGTH as u32),
            mapped_file: Option::None,
        })
    }

    pub fn load(file_path: &String) -> Result<Self> {
        let mut file = MmapStruct::load(file_path)?;
        check_crc(&file)?;

        let header = EntityHeader::read_from_mapped_file(&file, 0);
        let offset = ENTITY_HEADER_LENGTH;
        let data_type = map_data_type(file.get_u8(offset)).clone();
        let col_id = file.get_u8(offset + 1);

        Ok(VarLenStruct {
            path: file_path.clone(),
            header,
            data_type,
            col_id,
            initial_size: file.file_size,
            mapped_file: Option::Some(file),
            file: Option::None,
            file_size: AtomicU32::new(0),
            offset: AtomicU32::new(COLUMN_SEGMENT_HEADER_LENGTH as u32),
        })
    }

    pub fn write_next_bytes(&mut self, bytes: &Vec<u8>) -> Result<u32> {
        let base_offset = self.offset.load(std::sync::atomic::Ordering::Acquire);
        let rounded_length = (bytes.len() / 8 + 1) * 8;
        let next_offset = base_offset + 12 + rounded_length as u32;
        self.offset
            .store(next_offset, std::sync::atomic::Ordering::Release);

        let mut f = &mut self.file.as_mut().unwrap();
        f.seek(io::SeekFrom::Start(base_offset as u64))?;

        if (next_offset > self.file_size.load(std::sync::atomic::Ordering::Acquire) as u32) {
            self.file_size.store(
                self.file_size.load(std::sync::atomic::Ordering::Acquire) + 8192,
                std::sync::atomic::Ordering::Release,
            );
            f.set_len(self.file_size.load(std::sync::atomic::Ordering::Acquire) as u64)?;
        }

        f.write_all(&u64::to_le_bytes(next_offset as u64).to_vec())?;
        f.write_all(&u32::to_le_bytes(bytes.len() as u32).to_vec())?;
        f.write_all(&bytes)?;

        Ok(base_offset)
    }

    pub fn read_bytes(&self, offset: u32) -> Vec<u8> {
        let file = self.mapped_file.as_ref().unwrap();
        let len: usize = file.get_u32(offset as usize + 8) as usize;
        file.get_bytes(offset as usize + 12, len)
    }

    pub fn copy_bytes_val(&self, data: &mut Vec<u8>, offset: usize, to_offset: usize) -> usize {
        let file = self.mapped_file.as_ref().unwrap();
        let len: usize = file.get_u32(offset + 8) as usize;
        file.copy_bytes(data, offset + 12, to_offset, len);
        len
    }

    pub fn flush_and_remap(&mut self) -> Result<()> {
        match self.file.as_mut() {
            Some(mut f) => {
                f.sync_all()?;
                drop(f);
                self.file = Option::None;

                let mut file = MmapStruct::load(&self.path)?;
                put_crc(&mut file);
                file.flush()?;
                self.mapped_file = Option::Some(file);
            }
            None => {}
        }
        Ok(())
    }
}

impl Drop for VarLenStruct {
    fn drop(&mut self) {
        //println!("drop varl");
    }
}

#[macro_export]
macro_rules! column_descr {
    ($col_id:expr, $flags:expr, $dimension:expr, $data_type:expr, $name:expr) => {
        ColumnDescription {
            col_id: $col_id,
            flags: $flags,
            dimension: $dimension,
            data_type: $data_type,
            name: $name,
        }
    };
}

// RUSTFLAGS="-Awarnings" cargo test test_create_table_struct -- --nocapture
#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        collections::BTreeSet,
        fs,
        process::id,
        time::{Instant, SystemTime, UNIX_EPOCH},
        vec,
    };

    use rand::Rng;

    use crate::{
        db::state::{DB_STORAGE_VERSION, get_current_timestamp_millis},
        model::data_type::{DataType, INT_VECTOR, JSON, LONG, LONG_VECTOR, SHORT, TIMESTAMP},
        new_column,
    };

    use super::*;

    const PATH: &'static str = "/tmp/";
    const META_PATH: &'static str = "/tmp/ks_meta";

    const ENTITY_ID: u32 = 10;

    #[test]
    fn test_create_keyspace_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());

        let time = Instant::now().elapsed().as_millis() as u64;
        let mut string = String::new();

        let mut ks: KeyspaceStruct = KeyspaceStruct::new(
            &PATH.to_string(),
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            &"ks1".to_string(),
        )
        .unwrap();

        assert_eq!(ks.header.id, ENTITY_ID);
        assert_eq!(ks.header.created, time as u64);
        assert_eq!(ks.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(ks.name.as_bytes(), "ks1".to_string().as_bytes());

        let table = KeyspaceStruct::load(&PATH.to_string(), &meta_name);
        assert_eq!(ks.header.magic, MAGIC);
        assert_eq!(ks.header.id, ENTITY_ID);
        assert_eq!(ks.header.created, time as u64);
        assert_eq!(ks.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(ks.name.as_bytes(), "ks1".to_string().as_bytes());

        fs::remove_file(meta_name);
    }

    #[test]
    fn test_create_table_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(0x0, 0x1, 0, &LONG, "pk".as_bytes().to_vec()));
        columns.push(column_descr!(
            0x1,
            0x0,
            16,
            &LONG_VECTOR,
            "col2".as_bytes().to_vec()
        ));

        let time = Instant::now().elapsed().as_millis() as u64;

        let mut table: TableStruct = TableStruct::new(
            &PATH.to_string(),
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            &"table1".to_string(),
            12,
            &columns,
            1,
        )
        .unwrap();

        let d = table.columns.get(0).unwrap();
        assert_eq!(d.data_type.id, LONG.id);
        assert_eq!(d.col_id, 0x0);
        assert_eq!(d.flags, 0x1);
        assert_eq!(d.dimension, 0x0);
        assert_eq!(d.name, "pk".to_string().as_bytes());

        let d = table.columns.get(1).unwrap();
        assert_eq!(d.data_type.id, LONG_VECTOR.id);
        assert_eq!(d.col_id, 0x1);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 16);
        assert_eq!(d.name, "col2".to_string().as_bytes());

        assert_eq!(table.header.magic, MAGIC);
        assert_eq!(table.header.id, ENTITY_ID);
        assert_eq!(table.header.created, time as u64);
        assert_eq!(table.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(table.flags, 12);
        assert_eq!(table.name.as_bytes(), "table1".to_string().as_bytes());

        let table = TableStruct::load(&PATH.to_string(), &meta_name).unwrap();
        assert_eq!(table.header.magic, MAGIC);
        assert_eq!(table.header.id, ENTITY_ID);
        assert_eq!(table.header.created, time as u64);
        assert_eq!(table.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(table.flags, 12);
        assert_eq!(table.name.as_bytes(), "table1".to_string().as_bytes());

        let d = table.columns.get(0).unwrap();
        assert_eq!(d.data_type.id, LONG.id);
        assert_eq!(d.col_id, 0x0);
        assert_eq!(d.flags, 0x1);
        assert_eq!(d.dimension, 0);
        assert_eq!(d.name, "pk".to_string().as_bytes());

        let d = table.columns.get(1).unwrap();
        assert_eq!(d.data_type.id, LONG_VECTOR.id);
        assert_eq!(d.col_id, 0x1);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 16);
        assert_eq!(d.name, "col2".to_string().as_bytes());

        fs::remove_file(meta_name);
    }

    #[test]
    fn test_flat_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let time = Instant::now().elapsed().as_millis() as u64;

        let mut flat = FlatValStruct::new(
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            LONG,
            0,
            3,
            32,
        )
        .unwrap();

        assert_eq!(flat.header.magic, MAGIC);
        assert_eq!(flat.header.id, ENTITY_ID);
        assert_eq!(flat.header.created, time as u64);
        assert_eq!(flat.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(flat.data_type, LONG);
        assert_eq!(flat.dimension, 0);
        assert_eq!(flat.col_id, 3);
        assert_eq!(flat.row_limit, 32);

        flat.put_bytes_val(0, &u64::to_le_bytes(32).to_vec());
        flat.put_bytes_val(5, &u64::to_le_bytes(123).to_vec());

        flat.flush();

        let flat = FlatValStruct::load(&meta_name).unwrap();
        assert_eq!(flat.header.magic, MAGIC);
        assert_eq!(flat.header.id, ENTITY_ID);
        assert_eq!(flat.header.created, time as u64);
        assert_eq!(flat.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(flat.data_type, LONG);
        assert_eq!(flat.dimension, 0);
        assert_eq!(flat.col_id, 3);
        assert_eq!(flat.row_limit, 32);

        assert_eq!(flat.get_bytes_val(0), u64::to_le_bytes(32).to_vec());
        assert_eq!(flat.get_bytes_val(5), u64::to_le_bytes(123).to_vec());
        fs::remove_file(meta_name);
    }

    #[test]
    fn test_vector_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let time = Instant::now().elapsed().as_millis() as u64;
        let mut vector = VectorStruct::new(
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            INT_VECTOR,
            16,
            3,
            1024,
        )
        .unwrap();

        let vec_val: [u32; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 14, 15, 16, 17, 18, 19];

        assert_eq!(vector.header.magic, MAGIC);
        assert_eq!(vector.header.id, ENTITY_ID);
        assert_eq!(vector.header.created, time as u64);
        assert_eq!(vector.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(vector.data_type, INT_VECTOR);
        assert_eq!(vector.dimension, 16);
        assert_eq!(vector.col_id, 3);
        assert_eq!(vector.row_limit, 1024);
        assert_eq!(vector.row_size, 64);

        vector.put_u32_vector(128, &vec_val.to_vec());
        vector.flush();
        assert_eq!(vector.get_u32_vector(128), vec_val.to_vec());

        let mut vector = VectorStruct::load(&meta_name).unwrap();
        assert_eq!(vector.header.magic, MAGIC);
        assert_eq!(vector.header.id, ENTITY_ID);
        assert_eq!(vector.header.created, time as u64);
        assert_eq!(vector.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(vector.data_type, INT_VECTOR);
        assert_eq!(vector.dimension, 16);
        assert_eq!(vector.col_id, 3);
        assert_eq!(vector.row_limit, 1024);
        assert_eq!(vector.row_size, 64);

        assert_eq!(vector.get_u32_vector(128), vec_val.to_vec());

        fs::remove_file(meta_name);
    }

    #[test]
    fn test_var_len_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let time = Instant::now().elapsed().as_millis() as u64;

        let mut varl = VarLenStruct::new(
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            JSON,
            3,
            1024,
        )
        .unwrap();
        let val1: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let val2: [u8; 3] = [23, 54, 54];
        let val3: [u8; 9] = [1, 2, 3, 45, 4, 5, 6, 7, 8];

        let base_offset = varl.write_next_bytes(&val1.to_vec()).unwrap();
        let base_offset2 = varl.write_next_bytes(&val2.to_vec()).unwrap();
        let base_offset3 = varl.write_next_bytes(&val3.to_vec()).unwrap();

        assert_eq!(varl.header.magic, MAGIC);
        assert_eq!(varl.header.id, ENTITY_ID);
        assert_eq!(varl.header.created, time as u64);
        assert_eq!(varl.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(varl.data_type, JSON);
        assert_eq!(varl.col_id, 3);

        varl.flush_and_remap();

        let readed = varl.read_bytes(base_offset);
        let readed2 = varl.read_bytes(base_offset2);
        let readed3 = varl.read_bytes(base_offset3);
        assert_eq!(val1.to_vec(), readed);
        assert_eq!(val2.to_vec(), readed2);
        assert_eq!(val3.to_vec(), readed3);

        let varl = VarLenStruct::load(&meta_name).unwrap();
        assert_eq!(varl.header.magic, MAGIC);
        assert_eq!(varl.header.id, ENTITY_ID);
        assert_eq!(varl.header.created, time as u64);
        assert_eq!(varl.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(varl.data_type, JSON);
        assert_eq!(varl.col_id, 3);

        let readed = varl.read_bytes(base_offset);
        let readed2 = varl.read_bytes(base_offset2);
        let readed3 = varl.read_bytes(base_offset3);
        assert_eq!(val1.to_vec(), readed);
        assert_eq!(val2.to_vec(), readed2);
        assert_eq!(val3.to_vec(), readed3);

        fs::remove_file(meta_name);
    }

    #[test]
    fn test_segment_meta_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let time = Instant::now().elapsed().as_millis() as u64;

        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(
            0x0,
            0x1,
            0,
            &LONG_VECTOR,
            "pk".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x1,
            0x1,
            4,
            &LONG_VECTOR,
            "long_vec".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x2,
            0x0,
            0,
            &JSON,
            "json".as_bytes().to_vec()
        ));

        let mut meta = SegmentMetaStruct::new(
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            300,
            &columns,
        )
        .unwrap();

        assert_eq!(meta.header.magic, MAGIC);
        assert_eq!(meta.header.id, ENTITY_ID);
        assert_eq!(meta.header.created, time as u64);
        assert_eq!(meta.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(meta.rows_count, 300);
        assert_eq!(meta.col_var_len_mask, 4);
        assert_eq!(meta.col_mask, 7);

        let mut cols: Vec<Column> = Vec::new();
        cols.push(new_column!(
            0x0,
            LONG.id,
            0,
            0x1,
            u64::to_le_bytes(234).to_vec()
        ));
        let vec_val: Vec<u64> = [23, 34, 45, 67].to_vec();
        let mut vec_bytes: Vec<u8> = vec![];
        for v in vec_val {
            vec_bytes.extend(v.to_le_bytes());
        }
        cols.push(new_column!(0x1, LONG_VECTOR.id, 4, 0x0, vec_bytes));
        cols.push(new_column!(
            0x2,
            JSON.id,
            0,
            0x0,
            u64::to_le_bytes(234).to_vec()
        ));

        let mut var_l_data: Vec<VarLengthRowData> = Vec::new();
        var_l_data.push(VarLengthRowData {
            file_offset: 23,
            data_type: JSON.id,
            col_id: 0x3,
        });

        let offset1 = meta.insert_row_meta(23, 101, 200, false, &cols, &var_l_data);
        println!("offset {}", offset1);
        let offset2 = meta.insert_row_meta(24, 102, 202, false, &Vec::new(), &Vec::new());
        println!("offset {}", offset2);

        meta.flush_and_remap();

        assert_eq!(meta.header.magic, MAGIC);
        assert_eq!(meta.header.id, ENTITY_ID);
        assert_eq!(meta.header.created, time as u64);
        assert_eq!(meta.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(meta.rows_count, 300);
        assert_eq!(meta.col_var_len_mask, 4);
        assert_eq!(meta.col_mask, 7);

        let row_meta: RowMeta = meta.get_row_meta(offset1);

        assert_eq!(row_meta.idx, 23);
        assert_eq!(row_meta.created, 200);
        assert_eq!(row_meta.tx_id, 101);
        assert_eq!(row_meta.not_null_cols, 7);

        let row_meta: RowMeta = meta.get_row_meta(offset2);

        assert_eq!(row_meta.idx, 24);
        assert_eq!(row_meta.created, 202);
        assert_eq!(row_meta.tx_id, 102);
        assert_eq!(row_meta.not_null_cols, 0);

        let mut meta = SegmentMetaStruct::load(&meta_name, &columns).unwrap();

        let row_meta: RowMeta = meta.get_row_meta(offset1);

        assert_eq!(row_meta.idx, 23);
        assert_eq!(row_meta.created, 200);
        assert_eq!(row_meta.tx_id, 101);
        assert_eq!(row_meta.not_null_cols, 7);

        let row_meta: RowMeta = meta.get_row_meta(offset2);

        assert_eq!(row_meta.idx, 24);
        assert_eq!(row_meta.created, 202);
        assert_eq!(row_meta.tx_id, 102);
        assert_eq!(row_meta.not_null_cols, 0);

        fs::remove_file(meta_name);
    }

    #[test]
    fn test_segment_summary_struct() {
        let meta_name = format!("{}{}", META_PATH, getrandom::u64().unwrap());
        let mut columns: Vec<ColumnDescription> = Vec::new();
        columns.push(column_descr!(
            0x0,
            0x1,
            0,
            &LONG,
            "long_1".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x1,
            0x0,
            16,
            &LONG_VECTOR,
            "long_vec".as_bytes().to_vec()
        ));
        columns.push(column_descr!(
            0x2,
            0x0,
            0,
            &JSON,
            "varl_c".as_bytes().to_vec()
        ));

        let time = Instant::now().elapsed().as_millis() as u64;

        let from_vec: Vec<u64> = vec![555, 5557];
        let to_vec: Vec<u64> = vec![666, 6668];
        let from_bytes: Vec<u8> = vec![1, 2, 5, 6];
        let to_bytes: Vec<u8> = vec![11, 34, 21, 88];
        let range = PkRange {
            pk_num_from: 555,
            pk_num_to: 666,
            pk_composite_from: from_vec,
            pk_composite_to: to_vec,
            pk_bytes_from: from_bytes,
            pk_bytes_to: to_bytes,
            single_num: false,
        };

        let mut summary = SegmentSummaryStruct::new(
            &meta_name,
            &e_header!(ENTITY_ID, MAGIC, DB_STORAGE_VERSION, time),
            3,
            4,
            3,
            &columns,
            range,
        )
        .unwrap();

        assert_eq!(summary.header.magic, MAGIC);
        assert_eq!(summary.header.id, ENTITY_ID);
        assert_eq!(summary.header.created, time as u64);
        assert_eq!(summary.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(summary.tx_id_min, 3);
        assert_eq!(summary.tx_id_max, 4);
        assert_eq!(summary.rows_count, 3);

        let d = summary.columns.get(0).unwrap();
        assert_eq!(d.data_type.id, LONG.id);
        assert_eq!(d.col_id, 0x0);
        assert_eq!(d.flags, 0x1);
        assert_eq!(d.dimension, 0x0);
        assert_eq!(d.name, "long_1".to_string().as_bytes());

        let d = summary.columns.get(1).unwrap();
        assert_eq!(d.data_type.id, LONG_VECTOR.id);
        assert_eq!(d.col_id, 0x1);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 16);
        assert_eq!(d.name, "long_vec".to_string().as_bytes());

        let d = summary.columns.get(2).unwrap();
        assert_eq!(d.data_type.id, JSON.id);
        assert_eq!(d.col_id, 0x2);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 0);
        assert_eq!(d.name, "varl_c".to_string().as_bytes());

        let mut summary = SegmentSummaryStruct::load(&meta_name).unwrap();

        assert_eq!(summary.pk_range.single_num, false);
        assert_eq!(summary.pk_range.pk_num_from, 555);
        assert_eq!(summary.pk_range.pk_num_to, 666);

        assert_eq!(summary.header.magic, MAGIC);
        assert_eq!(summary.header.id, ENTITY_ID);
        assert_eq!(summary.header.created, time as u64);
        assert_eq!(summary.header.storage_version, DB_STORAGE_VERSION);
        assert_eq!(summary.tx_id_min, 3);
        assert_eq!(summary.tx_id_max, 4);
        assert_eq!(summary.rows_count, 3);

        let d = summary.columns.get(0).unwrap();
        assert_eq!(d.data_type.id, LONG.id);
        assert_eq!(d.col_id, 0x0);
        assert_eq!(d.flags, 0x1);
        assert_eq!(d.dimension, 0x0);
        assert_eq!(d.name, "long_1".to_string().as_bytes());

        let d = summary.columns.get(1).unwrap();
        assert_eq!(d.data_type.id, LONG_VECTOR.id);
        assert_eq!(d.col_id, 0x1);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 16);
        assert_eq!(d.name, "long_vec".to_string().as_bytes());

        let d = summary.columns.get(2).unwrap();
        assert_eq!(d.data_type.id, JSON.id);
        assert_eq!(d.col_id, 0x2);
        assert_eq!(d.flags, 0x0);
        assert_eq!(d.dimension, 0);
        assert_eq!(d.name, "varl_c".to_string().as_bytes());

        fs::remove_file(meta_name);
    }
}
