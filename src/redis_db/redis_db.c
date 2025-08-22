#include <stdlib.h>
#include <string.h>
#include "redis_db.h"
#include "../hash_table/hash_table.h"
#include "../lib/list.h"
#include "../streams/redis_stream.h"
#include "../channels/channel.h"
#include "../lib/sorted_set.h"
redis_db_t *redis_db_create(int id) {
    redis_db_t *db = calloc(1, sizeof(redis_db_t));
    if (!db) {
        return NULL;
    }
    
    db->id = id;
    
    db->dict = hash_table_create(1024);
    if (!db->dict) {
        free(db);
        return NULL;
    }
    
    db->expires = hash_table_create(256);
    if (!db->expires) {
        hash_table_destroy(db->dict);
        free(db);
        return NULL;
    }
    
    return db;
}

void redis_db_destroy(redis_db_t *db) {
    if (!db) return;
    
    if (db->dict) {
        // Destroy values (redis_object_t) stored in dict then destroy the table
        hash_table_destroy_with_free(db->dict, (void (*)(void *))redis_object_destroy);
    }
    
    if (db->expires) {
        hash_table_destroy(db->expires);
    }
    
    free(db);
}

redis_object_t *redis_object_create(redis_type_t type, void *ptr) {
    redis_object_t *obj = calloc(1, sizeof(redis_object_t));
    if (!obj) {
        return NULL;
    }

    obj->type = type;
    obj->ptr = ptr;
    obj->refcount = 1;
    obj->expiry = 0; /* default: no expiry */

    return obj;
}
redis_object_t *redis_object_create_string(const char *value) {
    char *str = strdup(value);
    if (!str) return NULL;
    
    return redis_object_create(REDIS_STRING, str);
}

redis_object_t *redis_object_create_list(void) {
    redis_list_t *list = list_create();
    if (!list) return NULL;
    
    return redis_object_create(REDIS_LIST, list);
}
redis_object_t *redis_object_create_stream(void *stream_ptr) {
    redis_object_t *obj = calloc(1, sizeof(redis_object_t));
    if (!obj) return NULL;
    
    obj->type = REDIS_STREAM;
    obj->ptr = stream_ptr;
    obj->refcount = 1;
    obj->expiry = 0;
    
    return obj;
}

redis_object_t *redis_object_create_channel(char *name) {
    channel_t *channel = create_channel(name);
    if (!channel) {
        return NULL;
    }
    
    return redis_object_create(REDIS_CHANNEL, channel);
}

redis_object_t *redis_object_create_number (const char *value)
{
    char *str = strdup(value);
    if(!str) return NULL;

    return redis_object_create(REDIS_NUMBER, str);
}

void redis_object_destroy(redis_object_t *obj) {
    if (!obj) return;
    
    obj->refcount--;
    if (obj->refcount > 0) {
        return; 
    }
    switch (obj->type) {
        case REDIS_STRING:
        case REDIS_NUMBER:
            free(obj->ptr);
            break;
        case REDIS_LIST:
            // List nodes may contain heap-allocated strings/objects; free them too
            list_destroy_with_free((redis_list_t *)obj->ptr, free);
            break;
        case REDIS_STREAM:
            redis_stream_destroy((redis_stream_t *)obj->ptr);
            break;
        case REDIS_CHANNEL:  
            destroy_channel((channel_t *)obj->ptr);
            break;
        case REDIS_ZSET:
           redis_sorted_set_destroy((redis_sorted_set_t *)obj->ptr);
           break;    
    }
    
    free(obj);
}

// Get string representation of Redis type
const char *redis_type_to_string(redis_type_t type) {
    switch (type) {
        case REDIS_STRING: return "string";
        case REDIS_LIST: return "list";
        case REDIS_STREAM: return "stream";
        case REDIS_ZSET: return "zset";
        case REDIS_CHANNEL: return "channel";
        default: return "unknown";
    }
}

redis_object_t *redis_object_create_sorted_set(void) {
    redis_sorted_set_t *zset = redis_sorted_set_create();
    if (!zset) return NULL;
    
    redis_object_t *obj = redis_object_create(REDIS_SORTED_SET, zset);
    if (!obj) {
        redis_sorted_set_destroy(zset);
        return NULL;
    }
    
    return obj;
}
