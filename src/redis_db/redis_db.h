#ifndef REDIS_DB_H
#define REDIS_DB_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "type_enums.h"
#include "../hash_table/hash_table.h"
#include <time.h>

typedef struct redis_object {
    redis_type_t type;     
    void *ptr;              
    int refcount;
    long long expiry;

} redis_object_t;

// Redis database structure
typedef struct redis_db {
    hash_table_t *dict;     
    hash_table_t *expires;  
    int id;                 
} redis_db_t;

// Database functions
redis_db_t *redis_db_create(int id);
void redis_db_destroy(redis_db_t *db);

// Redis object functions
redis_object_t *redis_object_create(redis_type_t type, void *ptr);
void redis_object_destroy(redis_object_t *obj);
redis_object_t *redis_object_create_string(const char *value);
redis_object_t *redis_object_create_list(void);
redis_object_t *redis_object_create_stream(void *stream_ptr);
redis_object_t *redis_object_create_number (const char *value);

// Utility function to get type name
const char *redis_type_to_string(redis_type_t type);
#endif