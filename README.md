# Redis Clone in C

A high-performance minimal Redis server clone built from scratch in C.  
Implements core Redis data structures, commands, persistence, and replication while maintaining low-level control over memory and concurrency.

---

##  Features

- **Event-driven architecture** using `epoll` for scalable, non-blocking I/O.
- **Core data structures & commands**: Strings, Lists, Streams, Sorted Sets.
- **Blocking operations**: `BLPOP`, `XREAD` with millisecond-precision timeouts.
- **Redis Streams** with radix tree indexing for efficient range queries.
- **Transactions**: Full support for `MULTI` / `EXEC` / `DISCARD` with atomic execution.
- **Persistence**: RDB-like snapshotting format to save the database to disk.
- **Replication**: Basic master-replica synchronization and command propagation.

---

##  Benchmarks

Tested with the official `redis-benchmark` tool:  

- Average throughput: **~60k requests/sec**  
- Stable under **200+ concurrent clients**  

Command set tested: `PING`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`.

---

##  Getting Started

### Prerequisites
- GCC or Clang
- Linux 
- `make`

### Clone the repository
```sh
git clone https://github.com/Momensamir12/Redis-Clone-.git
cd Redis-Clone-
```
---
### Run & Test locally

```sh
./run.sh
```
open another terminal and run 
```sh
redis-cli -p 6379 PING
redis-cli -p 6379 SET mykey hello
redis-cli -p 6379 GET mykey
```
or if you don't have redis installed 

```sh
printf '*1\r\n$4\r\nPING\r\n' | nc -q 1 127.0.0.1 6379
printf '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n' | nc -q 1 127.0.0.1 6379
printf '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n' | nc -N 127.0.0.1 6379
```

