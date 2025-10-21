// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};

use crate::db::DbMainState;
use crate::protocol::{
    ErrorCode, PROTOCOL_MAGIC, QueryResult, SqlErrorCode, parse_header, send_response,
};

use libc::{self, MSG_MORE};

#[allow(unused_macros)]
macro_rules! libc_call {
 ($fn:ident ( $($arg: expr),* $(,)* )) => {{
    let res: i32 = unsafe {libc::$fn($($arg, )*) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

const READ_FLAGS: i32 = libc::EPOLLIN | libc::EPOLLET;
const WRITE_FLAGS: i32 = libc::EPOLLOUT;
const CONNECT_FLAGS: i32 = libc::EPOLLIN;

const INCOMING_BUF_SIZE: usize = 0x1 << 22;
const OUTCOMING_BUF_SIZE: usize = 0x1 << 24;

pub struct EpollCtx {
    pub stream: TcpStream,
}

impl EpollCtx {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

pub fn start(mut db_state: DbMainState, port: u32) {
    let mut request_contexts: HashMap<i32, EpollCtx> = HashMap::new();
    let mut events: Vec<libc::epoll_event> = Vec::with_capacity(128);

    let connection_str = format!("127.0.0.1:{}", port);
    let listener =
        TcpListener::bind(&connection_str).expect(&format!("can't bind to {}", connection_str));
    listener
        .set_nonblocking(true)
        .expect("can't set server socker nonblocking");
    let server_fd = listener.as_raw_fd();
    println!("server fd {}", server_fd);

    let epoll_fd = epoll_create().expect("can create epoll queue");
    let event = libc::epoll_event {
        events: CONNECT_FLAGS as u32,
        u64: server_fd as u64,
    };

    ctrl_add(epoll_fd, server_fd, event).expect("can't create server socket");

    let mut incoming_buffer: Vec<u8> = [0; INCOMING_BUF_SIZE].to_vec();
    let mut outcoming_buffer: Vec<u8> = [0; OUTCOMING_BUF_SIZE].to_vec();

    loop {
        let wait_res = libc_call!(epoll_wait(
            epoll_fd,
            events.as_mut_ptr() as *mut libc::epoll_event,
            1024,
            1000 as libc::c_int,
        ));

        if (wait_res.is_err()) {
            println!("stop epoll");
            break;
        }

        let res = wait_res.unwrap();

        unsafe { events.set_len(res as usize) };

        for num in 0..res {
            let event = &events.get(num as usize).unwrap();
            let (e, fd) = (event.events, event.u64);

            if (fd as i32 == server_fd) {
                match listener.accept() {
                    Ok((stream, addr)) => {
                        println!("new client {}", addr);
                        let resp = stream.set_nonblocking(true);
                        match resp {
                            Ok(_) => {}
                            Err(e) => println!("can't setup connection as nonblocking"),
                        }

                        let resp = ctrl_add(
                            epoll_fd,
                            stream.as_raw_fd(),
                            libc::epoll_event {
                                events: READ_FLAGS as u32,
                                u64: stream.as_raw_fd() as u64,
                            },
                        );
                        match resp {
                            Ok(_) => {}
                            Err(e) => println!("can't make ctrl_add for connection {}", e),
                        }

                        let conn_fd = stream.as_raw_fd();
                        request_contexts.insert(conn_fd, EpollCtx::new(stream));
                    }
                    Err(e) => println!("error accept new connection {}", e),
                }
            } else if ((e & libc::EPOLLIN as u32) > 0) {
                let conn_fd: i32 = fd as i32;
                match request_contexts.get_mut(&conn_fd) {
                    Some(ev) => {
                        let read_result = ev.stream.read(&mut incoming_buffer);

                        match read_result {
                            Ok(l) => {
                                if l == 0 {
                                    drop_connection(
                                        fd as i32,
                                        &mut request_contexts,
                                        &mut db_state,
                                    );
                                } else if l < 8 || l == INCOMING_BUF_SIZE {
                                    send_response(
                                        &mut ev.stream,
                                        QueryResult::Simple {
                                            err: ErrorCode::PROTOCOL_BAD_MESSAGE_LENGTH,
                                        },
                                        &mut outcoming_buffer,
                                    );
                                } else {
                                    let mut rest_query = false;
                                    let header = parse_header(l, &incoming_buffer);

                                    if header.magic == PROTOCOL_MAGIC
                                        && l != header.length as usize + 8
                                    {
                                        send_response(
                                            &mut ev.stream,
                                            QueryResult::Simple {
                                                err: ErrorCode::PROTOCOL_BAD_MESSAGE_FORMAT,
                                            },
                                            &mut outcoming_buffer,
                                        );
                                        continue;
                                    }

                                    let mut start: usize = 8;
                                    let mut end: usize = header.length as usize + 8;
                                    if header.magic != 0xAA {
                                        //http rest req
                                        end = l;
                                        start = 0;
                                        rest_query = true;
                                    }

                                    let response = db_state.process_query(
                                        header,
                                        &incoming_buffer[start..end],
                                        &mut outcoming_buffer,
                                        conn_fd,
                                    );
                                    match response {
                                        Ok(r) => {
                                            send_response(&mut ev.stream, r, &mut outcoming_buffer);
                                        }
                                        Err(e) => {
                                            let (err, msg) =
                                                if let Some(err) = e.downcast_ref::<ErrorCode>() {
                                                    (*err, "internal error code".to_string())
                                                } else if let Some(err) =
                                                    e.downcast_ref::<SqlErrorCode>()
                                                {
                                                    (ErrorCode::SQL_SYNTAX_ERROR, e.to_string())
                                                } else {
                                                    (ErrorCode::SYSTEM_UNKNOWN, e.to_string())
                                                };

                                            if rest_query {
                                                send_response(
                                                    &mut ev.stream,
                                                    QueryResult::Plain {
                                                        response: msg.as_bytes().to_vec(),
                                                    },
                                                    &mut outcoming_buffer,
                                                );
                                            } else {
                                                send_response(
                                                    &mut ev.stream,
                                                    QueryResult::Error { err, msg },
                                                    &mut outcoming_buffer,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                println!("error reading data from socket {}", e.to_string());
                                drop_connection(fd as i32, &mut request_contexts, &mut db_state);
                            }
                        }
                    }
                    _ => println!("nothing for {}", fd),
                };
            }

            if (e & (libc::EPOLLHUP | libc::EPOLLRDHUP) as u32 > 0) {
                drop_connection(fd as i32, &mut request_contexts, &mut db_state);
            }
        }
    }
}

fn drop_connection(conn_fd: i32, context: &mut HashMap<i32, EpollCtx>, db_state: &mut DbMainState) {
    context.remove(&conn_fd);
    db_state.drop_connection(conn_fd);
    println!("conection closed {}", conn_fd)
}

fn epoll_create() -> io::Result<RawFd> {
    let fd = libc_call!(epoll_create1(0))?;
    Ok(fd)
}

fn close(fd: RawFd) {
    let _ = libc_call!(close(fd));
}

fn ctrl_add(epoll_fd: RawFd, fd: RawFd, mut event: libc::epoll_event) -> io::Result<()> {
    libc_call!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event))?;
    Ok(())
}

fn modify_interest(epoll_fd: RawFd, fd: RawFd, mut event: libc::epoll_event) -> io::Result<()> {
    libc_call!(epoll_ctl(epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut event))?;
    Ok(())
}

fn remove_interest(epoll_fd: RawFd, fd: RawFd) -> io::Result<()> {
    libc_call!(epoll_ctl(
        epoll_fd,
        libc::EPOLL_CTL_DEL,
        fd,
        std::ptr::null_mut()
    ))?;
    Ok(())
}

pub fn send_more(stream: &mut TcpStream, buf: *const u8, len: usize) -> io::Result<usize> {
    //println!("send more {}", len);
    let fd = stream.as_raw_fd();
    let rc = unsafe { libc::send(fd, buf as *const _, len, MSG_MORE) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(rc as usize)
}

pub fn send_last(stream: &mut TcpStream, buf: *const u8, len: usize) -> io::Result<usize> {
    //println!("send last {}", len);
    let fd = stream.as_raw_fd();
    let rc = unsafe { libc::send(fd, buf as *const _, len, 0) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(rc as usize)
}
