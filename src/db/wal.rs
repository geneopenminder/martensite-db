// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use crc32c::crc32c;
use memmap2::MmapMut;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};

use crate::db::state::{DB_STORAGE_VERSION, get_current_timestamp_millis};
use crate::model::column::Row;

/// WAL not finished yet
/// were used for benchmarks

#[derive(Clone)]
pub struct WalHeader {
    pub start_tx_id: u64,
    pub end_tx_id: u64,
    pub created: u64,
    pub closed: u64,
    pub storage_version: u32,
    pub records_count: u32,
    pub crc: u32,
}

//NOT THREAD SAFE
pub struct Wal {
    root: String,
    db_version: u32,
    start_id: u64,
    segment: Option<WalSegment>,
}

impl Wal {
    pub fn new(root: &String, db_version: u32) -> Self {
        Self {
            root: root.clone(),
            db_version: db_version,
            start_id: 0,
            segment: Option::None,
        }
    }

    pub fn write_record(&mut self, ks_id: u64, table_id: u64, row: &Row) {
        if (self.segment.is_none()) {
            self.start_id = get_current_timestamp_millis().unwrap() as u64;
            let mut segment = WalSegment::new(&self.root, self.start_id, row.tx_id);
            self.segment = Option::Some(segment);
        }

        let mut segment = self.segment.as_mut().unwrap();
        let size = segment.write_wal_record(ks_id, table_id, row);

        if (size > WAL_SEGMENT_SIZE_LIMIT) {
            segment.close(row.tx_id);
            self.start_id += 1;
            println!("new wal segment");
            let mut segment = WalSegment::new(&self.root, self.start_id, row.tx_id);
            self.segment = Option::Some(segment);
        }
    }

    pub fn close(&mut self, tx_id: u64) {
        println!("close wal");
        if (self.segment.is_some()) {
            self.segment.as_mut().unwrap().close(tx_id);
        }
    }
}

pub struct WalSegment {
    start_tx_id: u64,
    created: u64,
    file: File,
    records_count: u32,
    buf_offset: u32,
    file_offset: u32,
    file_size: u32,
    path: String,
    buf: Vec<u8>,
}

const WAL_SEGMENT_INITIAL_SIZE: u32 = 0x1 << 10;
const WAL_SEGMENT_SIZE_LIMIT: u32 = 0x1 << 22;
const WAL_PAGE_SIZE: u32 = 8192 * 4;

impl WalSegment {
    pub fn new(root: &String, id: u64, tx_id: u64) -> Self {
        let wal_path = format!("{}/{}.wal", root, id);
        let mut file = File::create(wal_path.clone()).unwrap();
        file.set_len(WAL_SEGMENT_INITIAL_SIZE as u64);

        let mut buf: Vec<u8> = [0; WAL_PAGE_SIZE as usize].to_vec();

        //skip header
        let offset = 40;
        let buf_offset = offset;

        let created = get_current_timestamp_millis().unwrap() as u64;

        WalSegment {
            start_tx_id: tx_id,
            created: created,
            file: file,
            records_count: 0,
            buf_offset: buf_offset as u32,
            file_offset: offset as u32,
            file_size: WAL_SEGMENT_INITIAL_SIZE,
            path: wal_path,
            buf: buf,
        }
    }

    fn wite_wal_header(&mut self, header: &WalHeader) {
        self.file.seek(io::SeekFrom::Start(0));
        self.file.write(&u64::to_le_bytes(header.start_tx_id));
        self.file.write(&u64::to_le_bytes(header.end_tx_id));
        self.file.write(&u64::to_le_bytes(header.created));
        self.file.write(&u64::to_le_bytes(header.closed));
        self.file.write(&u32::to_le_bytes(header.storage_version));
        self.file.write(&u32::to_le_bytes(header.records_count));
        self.file.write(&u32::to_le_bytes(header.storage_version));
        self.file.write(&u32::to_le_bytes(0));
    }

    pub fn read_wal_header(file: &mut File) -> WalHeader {
        file.seek(io::SeekFrom::Start(0));
        let mut buf: [u8; 8] = [0; 8];
        file.read_exact(&mut buf);
        let start_tx_id = u64::from_le_bytes(buf);
        file.read_exact(&mut buf);
        let end_tx_id = u64::from_le_bytes(buf);
        file.read_exact(&mut buf);
        let created = u64::from_le_bytes(buf);
        file.read_exact(&mut buf);
        let closed = u64::from_le_bytes(buf);

        let mut buf: [u8; 4] = [0; 4];
        file.read_exact(&mut buf);
        let storage_version = u32::from_le_bytes(buf);
        file.read_exact(&mut buf);
        let records_count = u32::from_le_bytes(buf);
        file.read_exact(&mut buf);
        let crc = u32::from_le_bytes(buf);
        WalHeader {
            start_tx_id,
            end_tx_id,
            created,
            closed,
            storage_version,
            records_count,
            crc,
        }
    }

    pub fn close(&mut self, tx_id: u64) -> String {
        let mut data = &mut self.buf;

        let base_offset = self.file_offset;
        let next_offset = base_offset + self.buf_offset as u32;
        if (next_offset > self.file_size) {
            self.file_size += WAL_PAGE_SIZE;
            self.file.set_len(self.file_size as u64);
        }

        self.file.seek(io::SeekFrom::Start(base_offset as u64));
        self.file.write_all(&data[0..self.buf_offset as usize]);

        self.file.seek(io::SeekFrom::Start(0));
        self.wite_wal_header(&WalHeader {
            start_tx_id: self.start_tx_id,
            end_tx_id: tx_id,
            created: self.created,
            closed: get_current_timestamp_millis().unwrap() as u64,
            storage_version: DB_STORAGE_VERSION,
            records_count: self.records_count,
            crc: 0,
        });

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .unwrap();
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

        let slice = &mmap[40..];
        crc32c(slice);
        self.file.sync_all();

        self.path.clone()
    }

    pub fn read_records(wal_path: &String) -> WalHeader {
        let mut file = File::open(wal_path.clone()).unwrap();
        let header = Self::read_wal_header(&mut file);
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let slice = &mmap[40..];
        let crc = crc32c(slice);
        if crc != header.crc {
            panic!("wal corrupted"); //TODO
        }
        file.seek(io::SeekFrom::Start(40));

        for i in 0..header.records_count {
            let mut ks_id: [u8; 8] = [0x0; 8];
            file.read_exact(&mut ks_id);
            let mut table_id: [u8; 8] = [0x0; 8];
            file.read_exact(&mut table_id);
            let mut tx_id: [u8; 8] = [0x0; 8];
            file.read_exact(&mut tx_id);

            println!(
                "rec {} tx_id {} table {}",
                i,
                u64::from_le_bytes(tx_id),
                u64::from_le_bytes(table_id)
            );

            let mut timestamp: [u8; 8] = [0x0; 8];
            file.read_exact(&mut timestamp);

            let mut deleted: [u8; 1] = [0x0; 1];
            file.read_exact(&mut deleted);
            let mut columns: [u8; 1] = [0x0; 1];
            file.read_exact(&mut columns);

            for c in 0..columns[0] {
                let mut col_id: [u8; 1] = [0x0; 1];
                file.read_exact(&mut col_id);
                let mut d_type: [u8; 1] = [0x0; 1];
                file.read_exact(&mut d_type);
                let mut flags: [u8; 1] = [0x0; 1];
                file.read_exact(&mut flags);
                let mut dimension: [u8; 2] = [0x0; 2];
                file.read_exact(&mut dimension);
                let mut val_len: [u8; 4] = [0x0; 4];
                file.read_exact(&mut val_len);

                let len = u32::from_le_bytes(val_len);

                println!(
                    "column {} id {} type {} flags {} dim {} val_len {}",
                    c,
                    col_id[0],
                    d_type[0],
                    flags[0],
                    u16::from_le_bytes(dimension),
                    len
                );

                let mut val = vec![0; len as usize];
                file.read_exact(&mut val);
            }
        }
        header
    }

    pub fn write_wal_record(&mut self, ks_id: u64, table_id: u64, row: &Row) -> u32 {
        self.records_count += 1;

        let mut buf_record_offset: usize = self.buf_offset as usize;

        let mut length: usize = row.payload.iter().map(|c| c.value.len()).sum();
        length += row.payload.len() * 9;
        length += 34;

        let mut data = &mut self.buf;

        if (buf_record_offset + length > WAL_PAGE_SIZE as usize) {
            let base_offset = self.file_offset;
            let next_offset = base_offset + buf_record_offset as u32;
            if (next_offset > self.file_size) {
                self.file_size += WAL_PAGE_SIZE;
                self.file.set_len(self.file_size as u64);
            }

            self.file_offset = next_offset;

            self.file.seek(io::SeekFrom::Start(base_offset as u64));
            //println!("write all {}", base_offset);
            self.file.write_all(&data[0..buf_record_offset]);
            self.file.sync_all();

            self.buf_offset = 0;
            buf_record_offset = 0;
        }
        //println!("payload len {}", length);

        data[buf_record_offset..buf_record_offset + 8].copy_from_slice(&u64::to_le_bytes(ks_id));
        buf_record_offset += 8;
        data[buf_record_offset..buf_record_offset + 8].copy_from_slice(&u64::to_le_bytes(table_id));
        buf_record_offset += 8;
        data[buf_record_offset..buf_record_offset + 8]
            .copy_from_slice(&u64::to_le_bytes(row.tx_id));
        buf_record_offset += 8;
        data[buf_record_offset..buf_record_offset + 8]
            .copy_from_slice(&u64::to_le_bytes(row.timestamp));
        buf_record_offset += 8;

        data[buf_record_offset] = if row.deleted { 1 } else { 0 };
        buf_record_offset += 1;

        data[buf_record_offset] = row.payload.len() as u8;
        buf_record_offset += 1;

        //for deleted will be only single pk (should be)
        for c in &row.payload {
            data[buf_record_offset] = c.col_id;
            buf_record_offset += 1;
            data[buf_record_offset] = c.d_type;
            buf_record_offset += 1;
            data[buf_record_offset] = c.flags;
            buf_record_offset += 1;

            data[buf_record_offset..buf_record_offset + 2]
                .copy_from_slice(&u16::to_le_bytes(c.dimension));
            buf_record_offset += 2;
            data[buf_record_offset..buf_record_offset + 4]
                .copy_from_slice(&u32::to_le_bytes(c.value.len() as u32));
            buf_record_offset += 4;
            data[buf_record_offset..buf_record_offset + c.value.len()].copy_from_slice(&c.value);
            buf_record_offset += c.value.len();
        }

        self.buf_offset = buf_record_offset as u32;

        self.file_offset
    }
}

// RUSTFLAGS="-Awarnings" cargo test test_create_table_struct -- --nocapture
#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        thread::sleep_ms,
        time::{Instant, SystemTime, UNIX_EPOCH},
        vec,
    };

    use crate::{
        data::index::PrimaryKey,
        model::{
            Column,
            data_type::{DataType, INT_VECTOR, JSON, LONG, LONG_VECTOR, SHORT},
        },
        new_column,
    };

    use super::*;

    const PATH: &'static str = "/tmp/";

    #[test]
    fn test_create_wal_segment() {
        let mut segment = WalSegment::new(&PATH.to_string(), 10, 0);

        let mut tx_id: u64 = 0;

        for i in 0..10 {
            let mut cols: Vec<Column> = Vec::new();
            let pk = Column {
                dimension: 0,
                d_type: LONG.id,
                col_id: 0x0,
                flags: 0x1,
                value: u64::to_le_bytes(i).to_vec(),
                num_val: i,
                placeholder: false,
            };
            cols.push(pk.clone());
            let vec_val: Vec<u8> = [
                23, 34, 45, 67, 34, 45, 56, 67, 23, 34, 45, 67, 34, 45, 56, 67,
            ]
            .to_vec();
            cols.push(Column {
                dimension: 2,
                d_type: LONG_VECTOR.id,
                col_id: 0x1,
                flags: 0x0,
                value: vec_val,
                num_val: 0,
                placeholder: false,
            });

            let row = Row {
                pk: PrimaryKey {
                    primary: cols.get(0).unwrap().clone(),
                    secondary: Option::None,
                },
                timestamp: get_current_timestamp_millis().unwrap() as u64,
                tx_id: i,
                payload: cols,
                deleted: false,
            };

            segment.write_wal_record(1, 1, &row);
        }

        let path = segment.close(9999);

        let mut file = File::open(&segment.path).unwrap();
        let header = WalSegment::read_wal_header(&mut file);

        assert_eq!(0, header.start_tx_id);
        assert_eq!(9999, header.end_tx_id);
        assert_eq!(DB_STORAGE_VERSION, header.storage_version);
        assert_eq!(10, header.records_count);

        fs::remove_file(segment.path);
    }

    #[test]
    fn test_wal() {
        let mut wal = Wal::new(&PATH.to_string(), 10);

        for i in 0..100 {
            let mut cols: Vec<Column> = Vec::new();
            let pk = Column {
                dimension: 0,
                d_type: LONG.id,
                col_id: 0x0,
                flags: 0x1,
                value: u64::to_le_bytes(i).to_vec(),
                num_val: i,
                placeholder: false,
            };
            cols.push(pk.clone());
            let vec_val: Vec<u8> = [
                23, 34, 45, 67, 34, 45, 56, 67, 23, 34, 45, 67, 34, 45, 56, 67,
            ]
            .to_vec();
            cols.push(Column {
                dimension: 2,
                d_type: LONG_VECTOR.id,
                col_id: 0x1,
                flags: 0x0,
                value: vec_val,
                num_val: 0,
                placeholder: false,
            });

            let row = Row {
                pk: PrimaryKey {
                    primary: cols.get(0).unwrap().clone(),
                    secondary: Option::None,
                },
                timestamp: get_current_timestamp_millis().unwrap() as u64,
                tx_id: i,
                payload: cols,
                deleted: false,
            };

            wal.write_record(1, 1, &row);
        }

        wal.close(9999);
    }
}
