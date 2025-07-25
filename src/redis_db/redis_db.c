#include <stdlib.h>
#include <string.h>
#include "redis_db.h"

// Create a new Redis database
redis_db_t *redis_db_create(int id) {
    redis_db_t *db = malloc(sizeof(redis_db_t));
    if (!db) {
        return NULL;
    }
    
    // Initialize database ID
    db->id = id;
    
    // Create main dictionary with reasonable initial size
    db->dict = hash_table_create(1024);
    if (!db->dict) {
        free(db);
        return NULL;
    }
    
    // Create expires dictionary (smaller initial size)
    db->expires = hash_table_create(256);
    if (!db->expires) {
        hash_table_destroy(db->dict);
        free(db);
        return NULL;
    }
    
    return db;
}

// Destroy a Redis database and all its contents
void redis_db_destroy(redis_db_t *db) {
    if (!db) return;
    
    // First, free all Redis objects in the main dictionary
    if (db->dict) {
        // Iterate through all buckets
        for (size_t i = 0; i < db->dict->size; i++) {
            hash_entry_t *entry = db->dict->buckets[i];
            while (entry) {
                // Free the Redis object
                redis_object_t *obj = (redis_object_t *)entry->value;
                redis_object_destroy(obj);
                entry = entry->next;
            }
        }
        
        // Destroy the hash table itself
        hash_table_destroy(db->dict);
    }
    
    // Clean up expires dictionary
    if (db->expires) {
        // Values in expires dict are timestamps (not Redis objects)
        // so they don't need special cleanup
        hash_table_destroy(db->expires);
    }
    
    // Free the database structure
    free(db);
}

// Create a Redis object
redis_object_t *redis_object_create(redis_type_t type, void *ptr) {
    redis_object_t *obj = malloc(sizeof(redis_object_t));
    if (!obj) {
        return NULL;
    }
    
    obj->type = type;
    obj->ptr = ptr;
    obj->refcount = 1;  // Start with reference count of 1
    
    return obj;
}

// Destroy a Redis object
void redis_object_destroy(redis_object_t *obj) {
    if (!obj) return;
    
    // Decrease reference count
    obj->refcount--;
    if (obj->refcount > 0) {
        return;  // Still referenced elsewhere
    }
    
    // Free the data based on type
    switch (obj->type) {
        case REDIS_STRING:
            free(obj->ptr);  // Free the string
            break;
            
        case REDIS_LIST:
            // TODO: Implement list_destroy((list_t *)obj->ptr);
            break;
            
        case REDIS_HASH:
            hash_table_destroy((hash_table_t *)obj->ptr);
            break;
            
        case REDIS_SET:
            // TODO: Implement set_destroy((set_t *)obj->ptr);
            break;
            
        case REDIS_ZSET:
            // TODO: Implement zset_destroy((zset_t *)obj->ptr);
            break;
    }
    
    // Free the Redis object itself
    free(obj);
}

// Get string representation of Redis type
const char *redis_type_to_string(redis_type_t type) {
    switch (type) {
        case REDIS_STRING: return "string";
        case REDIS_LIST:   return "list";
        case REDIS_HASH:   return "hash";
        case REDIS_SET:    return "set";
        case REDIS_ZSET:   return "zset";
        default:           return "unknown";
    }
}