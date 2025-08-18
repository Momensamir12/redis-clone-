#ifndef RDB_H
#define RDB_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h> 
#include "../lib/sds.h"         
#include "io_buffer.h"
#include "../redis_db/redis_db.h"
#include "../streams/redis_stream.h"

typedef struct {
    int fd;
    uint64_t crc;
    int dbnum;
} RDBLoader;

// Function declarations
size_t encode_int(io_buffer *rdb, int64_t val);
int is_string_representable_as_int(sds s); 
ssize_t rdb_write_raw(io_buffer *rdb, void *p, size_t len);
int rdb_save_len(io_buffer *rdb, uint32_t len);
ssize_t save_string(io_buffer *rdb, sds val);
uint32_t rdb_load_len(RDBLoader *loader);
int rdb_save_database(io_buffer *rdb, redis_db_t *db);
int save_key_value(io_buffer *rdb, sds key, redis_object_t *val);
int rdb_save_type(io_buffer *rdb, redis_object_t *obj);
int save_object(io_buffer *rdb, sds key, redis_object_t *obj);
int save_stream_entries(io_buffer *rdb, redis_stream_t *stream);
redis_stream_t *load_stream(RDBLoader *loader);
int load_stream_entry(RDBLoader *loader, redis_stream_t *stream);
int load_stream_entry_full(RDBLoader *loader, redis_db_t *db);
void rdb_load_full(const char *path, redis_db_t *db);
int load_string_entry(RDBLoader *loader, redis_db_t *db);
int load_list_entry(RDBLoader *loader, redis_db_t *db);


#endif