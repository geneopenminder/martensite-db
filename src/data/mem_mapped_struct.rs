// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::fs::{File, OpenOptions};
use std::ptr;

use libc::mlock;
use memmap2::MmapMut;

use anyhow::Result;
use crc32c::crc32c;

/// Every DB entity represented as memory mapped file
/// Some of them locked in memory

pub struct MmapStruct {
    pub path: String,
    pub file_size: u64,
    file: File,
    pub mmap: MmapMut,
}

impl MmapStruct {
    pub fn new_limited(path: &String, file_size: u64) -> Result<Self> {
        println!("mmaped path {} size {}", path, file_size);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        file.set_len(file_size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            path: path.clone(),
            file_size,
            file,
            mmap,
        })
    }

    pub fn load(path: &String) -> Result<Self> {
        println!("load mem mapped {}", path);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            path: path.clone(),
            file_size: mmap.len() as u64,
            file,
            mmap,
        })
    }

    pub fn load_mlock(path: &String) -> Result<Self> {
        println!("load mem mapped {}", path);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let ptr = mmap.as_ptr() as *const libc::c_void;
        let len = mmap.len();
        let locked = unsafe { mlock(ptr, len) };
        println!("mlock status {}", locked);
        unsafe {
            let _ = libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_WILLNEED,
            );
        }
        Ok(Self {
            path: path.clone(),
            file_size: mmap.len() as u64,
            file,
            mmap,
        })
    }

    pub fn calculate_crc(&self, offset: usize) -> u32 {
        let slice = &self.mmap[offset..];
        crc32c(slice)
    }

    pub fn calculate_crc_limited(&self, end_offset: usize) -> u32 {
        let slice = &self.mmap[0..end_offset];
        crc32c(slice)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn get_u8(&self, offset: usize) -> u8 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const u8;
            u8::from_le(ptr::read(val))
        }
    }

    pub fn get_u16(&self, offset: usize) -> u16 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const u16;
            u16::from_le(ptr::read(val))
        }
    }

    pub fn get_u32(&self, offset: usize) -> u32 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const u32;
            u32::from_le(ptr::read(val))
        }
    }

    pub fn get_u64(&self, offset: usize) -> u64 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const u64;
            u64::from_le(ptr::read(val))
        }
    }

    pub fn get_f32(&self, offset: usize) -> f32 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const [u8; 4];
            f32::from_le_bytes(ptr::read(val))
        }
    }

    pub fn get_f64(&self, offset: usize) -> f64 {
        unsafe {
            let val = self.mmap.as_ptr().add(offset) as *const [u8; 8];
            f64::from_le_bytes(ptr::read(val))
        }
    }

    pub fn put_u8(&self, offset: usize, val: u8) {
        unsafe {
            let p: *mut u8 = self.mmap.as_ptr().add(offset) as *mut u8;
            ptr::write(p, val);
        }
    }

    pub fn put_u16(&self, offset: usize, val: u16) {
        unsafe {
            let p: *mut u16 = self.mmap.as_ptr().add(offset) as *mut u16;
            ptr::write(p, u16::to_le(val));
        }
    }

    pub fn put_u32(&self, offset: usize, val: u32) {
        unsafe {
            let p: *mut u32 = self.mmap.as_ptr().add(offset) as *mut u32;
            ptr::write(p, u32::to_le(val));
        }
    }

    pub fn put_u64(&mut self, offset: usize, val: u64) {
        unsafe {
            let p: *mut u64 = self.mmap.as_ptr().add(offset) as *mut u64;
            ptr::write(p, u64::to_le(val));
        }
    }

    pub fn put_f32(&self, offset: usize, val: f32) {
        unsafe {
            let p: *mut [u8; 4] = self.mmap.as_ptr().add(offset) as *mut [u8; 4];
            ptr::write(p, f32::to_le_bytes(val));
        }
    }

    pub fn put_f64(&self, offset: usize, val: f64) {
        unsafe {
            let p: *mut [u8; 8] = self.mmap.as_ptr().add(offset) as *mut [u8; 8];
            ptr::write(p, f64::to_le_bytes(val));
        }
    }

    pub fn put_string_with_size(&mut self, offset: usize, val: &[u8]) {
        unsafe {
            let p: *mut u8 = self.mmap.as_ptr().add(offset) as *mut u8;
            ptr::write(p, val.len() as u8);
            self.mmap[offset + 1..offset + 1 + val.len()].copy_from_slice(val);
        }
    }

    pub fn get_sized_string(&self, offset: usize) -> (u8, &[u8]) {
        unsafe {
            let p: *const u8 = self.mmap.as_ptr().add(offset) as *const u8;
            let len = u8::from_le(ptr::read(p));
            (len, &self.mmap[offset + 1..offset + 1 + len as usize])
        }
    }

    pub fn put_bytes(&mut self, offset: usize, bytes: &Vec<u8>) {
        unsafe {
            self.mmap[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
    }

    pub fn get_bytes(&self, offset: usize, size: usize) -> Vec<u8> {
        self.mmap[offset..offset + size].to_vec()
    }

    pub fn put_u8_vector(&mut self, offset: usize, bytes: Vec<u8>) {
        unsafe {
            self.mmap[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
    }

    pub fn put_u16_vector(&mut self, base_offset: usize, vec: &Vec<u16>) {
        unsafe {
            for (i, v) in vec.iter().enumerate() {
                let p: *mut u16 = self.mmap.as_ptr().add(base_offset + i * 2) as *mut u16;
                ptr::write(p, u16::to_le(*v));
            }
        }
    }

    pub fn put_u32_vector(&mut self, base_offset: usize, vec: &Vec<u32>) {
        unsafe {
            for (i, v) in vec.iter().enumerate() {
                let p: *mut u32 = self.mmap.as_ptr().add(base_offset + i * 4) as *mut u32;
                ptr::write(p, u32::to_le(*v));
            }
        }
    }

    pub fn put_u64_vector(&mut self, base_offset: usize, vec: &Vec<u64>) {
        unsafe {
            for (i, v) in vec.iter().enumerate() {
                let p: *mut u64 = self.mmap.as_ptr().add(base_offset + i * 8) as *mut u64;
                ptr::write(p, u64::to_le(*v));
            }
        }
    }

    pub fn put_f32_vector(&mut self, base_offset: usize, vec: &Vec<f32>) {
        unsafe {
            for (i, v) in vec.iter().enumerate() {
                let p: *mut [u8; 4] = self.mmap.as_ptr().add(base_offset + i * 4) as *mut [u8; 4];
                ptr::write(p, f32::to_le_bytes(*v));
            }
        }
    }

    pub fn put_f64_vector(&mut self, base_offset: usize, vec: &Vec<f64>) {
        unsafe {
            for (i, v) in vec.iter().enumerate() {
                let p: *mut [u8; 8] = self.mmap.as_ptr().add(base_offset + i * 4) as *mut [u8; 8];
                ptr::write(p, f64::to_le_bytes(*v));
            }
        }
    }

    pub fn get_u8_vector(&self, offset: usize, dimension: u16) -> Vec<u8> {
        unsafe { self.mmap[offset..offset + dimension as usize].to_vec() }
    }

    pub fn get_u16_vector(&self, offset: usize, dimension: u16) -> Vec<u16> {
        let mut vector: Vec<u16> = vec![];
        unsafe {
            for i in 0..dimension {
                let val = self.mmap.as_ptr().add(offset + (i * 4) as usize) as *const [u8; 2];
                vector.push(u16::from_le_bytes(ptr::read(val)));
            }
        }

        vector
    }

    pub fn get_u32_vector(&self, offset: usize, dimension: u16) -> Vec<u32> {
        let mut vector: Vec<u32> = vec![];
        unsafe {
            for i in 0..dimension {
                let val = self.mmap.as_ptr().add(offset + (i * 4) as usize) as *const [u8; 4];
                vector.push(u32::from_le_bytes(ptr::read(val)));
            }
        }

        vector
    }

    pub fn get_u64_vector(&self, offset: usize, dimension: u16) -> Vec<u64> {
        let mut vector: Vec<u64> = vec![];
        unsafe {
            for i in 0..dimension {
                let val = self.mmap.as_ptr().add(offset + (i * 8) as usize) as *const [u8; 8];
                vector.push(u64::from_le_bytes(ptr::read(val)));
            }
        }

        vector
    }

    pub fn get_f32_vector(&self, offset: usize, dimension: u16) -> Vec<f32> {
        let mut vector: Vec<f32> = vec![];
        unsafe {
            for i in 0..dimension {
                let val = self.mmap.as_ptr().add(offset + (i * 4) as usize) as *const [u8; 4];
                vector.push(f32::from_le_bytes(ptr::read(val)));
            }
        }

        vector
    }

    pub fn get_f64_vector(&self, offset: usize, dimension: u16) -> Vec<f64> {
        let mut vector: Vec<f64> = vec![];
        unsafe {
            for i in 0..dimension {
                let val = self.mmap.as_ptr().add(offset + (i * 4) as usize) as *const [u8; 8];
                vector.push(f64::from_le_bytes(ptr::read(val)));
            }
        }

        vector
    }

    pub fn copy_bytes(
        &self,
        data: &mut Vec<u8>,
        from_offset: usize,
        to_offset: usize,
        size: usize,
    ) {
        unsafe {
            ptr::copy_nonoverlapping(
                self.mmap.as_ptr().add(from_offset),
                data.as_mut_ptr().add(to_offset),
                size,
            )
        };
    }
}
