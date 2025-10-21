# Martensite-DB

## What is Martensite-DB?

ProjectName is an experimental NoSQL, single-threaded, append-only database written in Rust. \
Inspired by Cassandra and Scylla — and designed for extreme performance. \
It uses a columnar storage layout to enable future support for FAISS vector indexes, making it well-suited for analytical and AI-driven workloads. \
A core design choice is that all data and indexes are stored directly in memory-mapped files, with no internal object representation. \
This approach allows the database to write data directly to the socket buffer with near-zero overhead, pushing it toward a true zero-copy architecture. \
There is no internal cache — the system relies solely on the operating system’s page cache. \
Open adressing hash map used as primary key index, locates in memory mapped files. 

Key features:

🚀 Prepared statements for efficient query execution

🔑 Unique composite numeric primary key index

📈 Range index support

🧠 Zero-copy I/O through direct memory-mapped access

PS: what is martensite - [wiki](https://en.wikipedia.org/wiki/Martensite)
## Running

```bash
$ cargo build --release
$ ./target/release/martensite-db --port 8081 --db-root /tmp/db
```
Default port is 8081, DB root is /tmp/db/
This will start a DB node as single thread app

## Testing

martensite-db supports http queries

```bash
$ curl -X GET "http://127.0.0.1:8081/query?create%20table%20ks1.long_vectors%20(pk%20long,vec%20long_vector\[8\],uud%20json,%20%20primary%20key%20(pk))"
or
$ ./python3 sql_python.py -- more useful for plain text queries
```
- create keyspace
``` sql
create keyspace ks1
```

- create table
``` sql
create table ks1.long_vectors (pk long,vec long_vector[8],text varchar, primary key (pk))
```

- insert some data
``` sql
insert into ks1.long_vectors (pk,vec,text) values (500,'23,34,56,78,1,2,3,4','dfghfghdfghdfghdfghfgdhdg')
```

- try select
``` sql
select * from ks1.long_vectors where pk = 500
select * from ks1.long_vectors where pk in (100,200)
```

## Benchmark

100k RPS on write for single thread.
250+ RPS on read for single thread with payload size 100 bytes
It's close to Redis and faster that any other databases
tested on laptop with Core i7 CPU

use - https://github.com/geneopenminder/rust-db-bench

for writing your own use client driver from client.rs source file

## Supported SQL clauses & data types

``` sql
CREATE KEYSPACE/CREATE TABLE
SELECT/INSERT/DELETE

-- data types

-- unsigned numeric
BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, TIMESTAMP (similar to LONG)
-- vector types
-- for vector always need to set fixed dimension long_vector[8] -- 8 elements of LONG
BYTE_VECTOR, SHORT_VECTOR, INT_VECTOR, LONG_VECTOR, FLOAT_VECTOR, DOUBLE_VECTOR

-- blob
BLOB
-- variable length string
VARCHAR
-- for val length columns tou can set dimension if data has fixed size - varchar[16] 

-- table with single primary key
-- ALL Primary key column should be simple numeric type - SHORT, INT, LONG, TIMESTAMP
create table ks1.long_vectors (pk long, text varchar, primary key (pk))
-- table with composite primary key
create table ks1.long_vectors (pk_1 long, pk_2 int, ts timestamp, text varchar, primary key (pk_1, pk_2, ts))

-- create range index allowed only for new table
create table ks1.time_series (pk long,ts long,uud varchar, primary key (pk,ts))  with (range_idx=true)

--after that you can use queries like that
select * from ks1.time_series where pk = 100 AND ts = 400
select * from ks1.time_series where pk = 100 AND ts in (200,300,400)
select * from ks1.time_series where pk = 100 AND ts > 300

```

## Documentation

driver binary protocol described in the file
[/src/client.rs](client)

documentation you can find in the different source code files


## Contact
* https://www.linkedin.com/in/evgeniy-s-pshenitsin/
* mail to <geneopenminder@gmail.com>