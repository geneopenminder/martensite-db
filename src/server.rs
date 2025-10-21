// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::{
    io::{Read, Write},
    net::*,
};

pub fn start() {
    let listener = TcpListener::bind("127.0.0.1:8081").unwrap();

    println!("server started!");

    //            buffer.put((byte)0x01); //magic
    //        buffer.put((byte)0x01); //version
    //        buffer.put((byte)0x00); //flags
    //        buffer.put((byte)0x03); //exec prepared

    //            buffer.putInt(length + 8); //length

    //          buffer.putInt(stId);
    //        buffer.putInt(values.length);
    //
    //          putValues(buffer);

    //        buffer.flip();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        println!("Connection established!");
        let mut buffer = [0; 512]; // A small buffer to read data into
        loop {
            let readed = stream.read(&mut buffer).unwrap();

            //println!("Received bytes: {}", readed);
            if (readed == 0) {
                return;
            }

            //let db_version: u32 = u32::from_le_bytes(received_data[0..4].try_into().unwrap());
            //println!("db_version: {}", db_version);

            let magic = buffer[0];
            let version = buffer[1];
            let flags = buffer[2];
            let cmd = buffer[3];
            let length = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
            if (length > 4) {
                let mut v: Vec<u8> = vec![];

                let start_index: usize = 8;

                /*
                let part_slice = &buffer[start_index..length as usize];


                let header: Header = Header{magic, version, flags, cmd};
                let h = &header;

                let mut data = part_slice.to_vec();

                //println!("header {h:?}");
                let p: Payload = Payload { header, data};
                */
            }

            let mut output = vec![0u8; 16];
            let out_size: u32 = 8;
            let out_size_bytes = out_size.to_le_bytes();
            output[4..8].copy_from_slice(&out_size_bytes);

            output[12] = 1;

            stream.write(&output);
        }
    }
}
