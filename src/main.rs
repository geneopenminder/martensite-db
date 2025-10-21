// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

mod client;
mod data;
mod db;
mod epoll_server;
mod model;
mod protocol;
mod vector_search;

use signal_hook::iterator::Signals;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::{env, thread};

use crate::db::DbMainState;

#[derive(Clone, Debug)]
struct Config {
    port: u32,
    memtable_size: u32,
    wal_size: u32,
    db_root: String,
}

fn parse_args() -> Config {
    let mut port = 8081;
    let mut memtable_size = 0x1 << 27;
    let mut wal_size = 0x1 << 25;
    let mut db_root: String = "/tmp/db/".to_string();

    let mut it = env::args().skip(1);
    while let Some(k) = it.next() {
        match k.as_str() {
            "--port" => {
                port = it.next().and_then(|v| v.parse().ok()).unwrap_or(port);
            }
            "--memtable-size" => {
                memtable_size = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(memtable_size);
            }
            "--wal-size" => {
                wal_size = it.next().and_then(|v| v.parse().ok()).unwrap_or(wal_size);
            }
            "--db-root" => {
                db_root = it.next().unwrap_or("/tmp/db/".to_string());
            }
            _ => {}
        }
    }

    Config {
        port: port,
        memtable_size: memtable_size,
        wal_size: wal_size,
        db_root: db_root,
    }
}

fn main() {
    println!("Start martensite-db!");

    let cfg = parse_args();

    println!("cfg {:?}", cfg);

    unsafe {
        let term_now = Arc::new(AtomicBool::new(false));
        let mut signals =
            Signals::new(&[signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM]).unwrap();

        thread::spawn(move || {
            for signal in signals.forever() {
                match signal {
                    signal_hook::consts::SIGINT | signal_hook::consts::SIGTERM => {
                        println!("Received termination signal. Shutting down with memtable flush");
                        break;
                    }
                    _ => unreachable!(),
                }
            }
        });

        epoll_server::start(
            DbMainState::init(cfg.db_root).expect("Can't start db"),
            cfg.port,
        );
    }
}
