# Redis Clone in C

A high-performance minimal Redis server clone built from scratch in C.  
Implements core Redis data structures, commands, persistence, and replication while maintaining low-level control over memory and concurrency.

---

## 🚀 Features

- **Event-driven architecture** using `epoll` for scalable, non-blocking I/O.
- **Core data structures & commands**: Strings, Lists, Sets, Sorted Sets.
- **Blocking operations**: `BLPOP`, `XREAD` with millisecond-precision timeouts.
- **Redis Streams** with radix tree indexing for efficient range queries.
- **Transactions**: Full support for `MULTI` / `EXEC` / `DISCARD` with atomic execution.
- **Persistence**: RDB-like snapshotting format to save the database to disk.
- **Replication**: Basic master-replica synchronization and command propagation.

---

## 📊 Benchmarks

Tested with the official `redis-benchmark` tool:  

- Average throughput: **~60k requests/sec**  
- Stable under **200+ concurrent clients**  

Command set tested: `PING`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`.

---

## 📦 Getting Started

### Prerequisites
- GCC or Clang
- Linux 
- `make`

### Build
```bash
./your_program.sh 
