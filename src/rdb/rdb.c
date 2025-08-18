#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "../lib/sds.h"
#include "io_buffer.h"
#include "rdb.h"
#include "../redis_db/type_enums.h"
#include "../lib/list.h"
#include "../streams/redis_stream.h"
#include "../lib/radix_tree.h"

#define RDB_ENC_INT8 0xF0
#define RDB_TYPE_STRING 0x00
#define RDB_TYPE_LIST 0x01
#define RDB_TYPE_SET 0x02
#define RDB_TYPE_ZSET 0x03
#define RDB_TYPE_HASH 0x04
#define RDB_TYPE_STREAM 0x0F

int is_string_representable_as_int(sds s)
{
    if (sdslen(s) == 0)
        return 0; 

    char *end;
    strtoll(s, &end, 10);
    return *end == '\0';
}

int rdb_save_type(io_buffer *rdb, redis_object_t *obj)
{
    uint8_t value_type;

    switch (obj->type)
    {
    case REDIS_STRING:
    case REDIS_NUMBER:
        value_type = RDB_TYPE_STRING;
        break;
    case REDIS_LIST:
        value_type = RDB_TYPE_LIST;
        break;
    case REDIS_SET:
        value_type = RDB_TYPE_SET;
        break;
    case REDIS_HASH:
        value_type = RDB_TYPE_HASH;
        break;
    case REDIS_STREAM:
        value_type = RDB_TYPE_STREAM;
        break;
    default:
        return -1;
    }
    return rdb_write_raw(rdb, &value_type, 1);
}

ssize_t rdb_write_raw(io_buffer *rdb, void *p, size_t len)
{
    if (rdb && buffer_write(rdb, p, len) == 0)
        return -1;
    return len;
}
int save_object(io_buffer *rdb, sds key, redis_object_t *obj)
{
    switch (obj->type)
    {
    case REDIS_STRING:
    {
        char *c_str = (char *)obj->ptr; 
        sds val = sdsnew(c_str);        

        if (is_string_representable_as_int(val))
        {
            int64_t intval = strtoll(val, NULL, 10);
            sdsfree(val);
            return encode_int(rdb, intval);
        }
        else
        {
            ssize_t result = save_string(rdb, val);
            sdsfree(val);
            return result;
        }
    }
    case REDIS_NUMBER:
    {
        char *c_str = (char *)obj->ptr;
        int64_t intval = strtoll(c_str, NULL, 10);
        return encode_int(rdb, intval);
    }
    case REDIS_LIST:
    {
        redis_list_t *list = (redis_list_t *)(obj->ptr);
        rdb_save_len(rdb, list->length);

        if (list->length == 0)
        {
            return 0; 
        }

        int count;
        char **result = list_range(list, 0, list->length - 1, &count);
        if (!result)
        {
            return -1; 
        }

        for (int i = 0; i < count; i++)
        { 
            sds temp = sdsnew(result[i]);
            ssize_t bytes_written = save_string(rdb, temp);
            sdsfree(temp); 

            if (bytes_written == -1)
            {
                free(result);
                return -1; 
            }
        }
        free(result);
        return count; 
    }
    case REDIS_STREAM:
{
    redis_stream_t *stream = (redis_stream_t *)(obj->ptr);
    
    if (rdb_save_len(rdb, stream->length) == -1) {
        return -1;
    }
    
    // Save last_id if it exists
    if (stream->last_id) {
        if (save_string(rdb, sdsnew(stream->last_id)) == -1) {
            return -1;
        }
    } else {
        // Save empty string for last_id
        if (save_string(rdb, sdsempty()) == -1) {
            return -1;
        }
    }
    
    // Save max_len
    if (rdb_save_len(rdb, stream->max_len) == -1) {
        return -1;
    }
    

    return save_stream_entries(rdb, stream);
}
    }
    return -1;
}

int save_stream_entries(io_buffer *rdb, redis_stream_t *stream)
{
    if (!stream || !stream->entries_tree) {
        return 0; // Empty stream
    }
    
    // Get all entries using range function (from smallest to largest ID)
    void **results = NULL;
    int count = 0;
    
    radix_tree_range(stream->entries_tree, "-", "+", &results, &count);
    
    if (!results) {
        return 0; 
    }
    
    for (int i = 0; i < count; i++) {
        stream_entry_t *entry = (stream_entry_t *)results[i];
        
        sds id_sds = sdsnew(entry->id);
        if (save_string(rdb, id_sds) == -1) {
            sdsfree(id_sds);
            free(results);
            return -1;
        }
        sdsfree(id_sds);
        
        // Save number of fields
        if (rdb_save_len(rdb, entry->field_count) == -1) {
            free(results);
            return -1;
        }
        
        // Save each field-value pair
        for (size_t j = 0; j < entry->field_count; j++) {
            // Save field name
            sds field_name = sdsnew(entry->fields[j].name);
            if (save_string(rdb, field_name) == -1) {
                sdsfree(field_name);
                free(results);
                return -1;
            }
            sdsfree(field_name);
            
            // Save field value
            sds field_value = sdsnew(entry->fields[j].value);
            if (save_string(rdb, field_value) == -1) {
                sdsfree(field_value);
                free(results);
                return -1;
            }
            sdsfree(field_value);
        }
    }
    
    free(results);
    return 0;
}

int save_key_value(io_buffer *rdb, sds key, redis_object_t *obj)
{
    // 1. Save value type
    if (rdb_save_type(rdb, obj) == -1)
    {
        return -1;
    }

    // 2. Save key
    if (save_string(rdb, key) == -1)
    {
        return -1;
    }

    // 3. Save value
    return save_object(rdb, key, obj);
}

int rdb_save_len(io_buffer *rdb, uint32_t len)
{
    unsigned char buf[5];
    size_t nwritten = 0;

    if (len < (1 << 6))
    {
        /* 6-bit len: 00LLLLLL */
        buf[0] = (len & 0x3F);
        nwritten = 1;
    }
    else if (len < (1 << 14))
    {
        /* 14-bit len: 01LLLLLL LLLLLLLL */
        buf[0] = ((len >> 8) & 0x3F) | 0x40;
        buf[1] = len & 0xFF;
        nwritten = 2;
    }
    else if (len <= 0xFFFFFFFF)
    {
        /* 32-bit len: 10...... [4-byte len] */
        buf[0] = 0x80;
        if (rdb_write_raw(rdb, buf, 1) == -1)
            return -1;
        uint32_t len32 = htonl(len);
        if (rdb_write_raw(rdb, &len32, 4) == -1)
            return -1;
        return 5;
    }

    return (rdb_write_raw(rdb, buf, nwritten) == -1) ? -1 : nwritten;
}

ssize_t save_string(io_buffer *rdb, sds val)
{
    if (rdb_save_len(rdb, sdslen(val)) == -1)
    {
        return -1;
    }
    return buffer_write(rdb, val, sdslen(val));
}

size_t encode_int(io_buffer *rdb, int64_t val)
{
    if (val >= INT8_MIN && val <= INT8_MAX)
    {
        unsigned char buf[2];
        buf[0] = RDB_TYPE_STRING;
        buf[1] = val & 0xFF;
        return buffer_write(rdb, buf, 2);
    }
    else
    {
        // Convert large int to SDS string
        sds int_str = sdsfromlonglong(val);
        ssize_t bytes_written = save_string(rdb, int_str);
        sdsfree(int_str);
        return bytes_written;
    }
}


void rdb_load_full(const char *path, redis_db_t *db)
{
    if (!db) {
        fprintf(stderr, "Database parameter is NULL\n");
        return;
    }

    RDBLoader loader;
    loader.fd = open(path, O_RDONLY);
    if (loader.fd == -1)
    {
        perror("open");
        return;
    }

    // 1. Verify magic number
    char magic[9];
    if (read(loader.fd, magic, 9) != 9 || memcmp(magic, "REDIS", 5) != 0)
    {
        fprintf(stderr, "Invalid RDB file\n");
        close(loader.fd);
        return;
    }

    printf("RDB version: %.4s\n", magic + 5);

    // 2. Main parsing loop
    while (1)
    {
        unsigned char type;
        if (read(loader.fd, &type, 1) != 1)
            break;

        if (type == 0xFF)
        { // EOF marker
            printf("End of RDB file\n");
            break;
        }

        switch (type)
        {
        case 0xFE:
        { // Database selector
            uint8_t dbnum;
            read(loader.fd, &dbnum, 1);
            loader.dbnum = dbnum;
            printf("Selecting DB %d\n", dbnum);
            break;
        }
        case RDB_ENC_INT8:
        {
            
        }
        case RDB_TYPE_STRING: // 0x00
        {
            if (load_string_entry(&loader, db) == -1) {
                fprintf(stderr, "Failed to load string entry\n");
                close(loader.fd);
                return;
            }
            break;
        }
        case RDB_TYPE_LIST: // 0x01
        {
            if (load_list_entry(&loader, db) == -1) {
                fprintf(stderr, "Failed to load list entry\n");
                close(loader.fd);
                return;
            }
            break;
        }
        case RDB_TYPE_STREAM: // 0x0F
        {
            if (load_stream_entry_full(&loader, db) == -1) {
                fprintf(stderr, "Failed to load stream entry\n");
                close(loader.fd);
                return;
            }
            break;
        }
        default:
            fprintf(stderr, "Unknown/Unsupported type: 0x%02X\n", type);
            // Try to skip this entry - this is risky but better than crashing
            break;
        }
    }
    
    printf("Successfully loaded RDB file. Database now contains %zu keys.\n", db->dict->count);
    close(loader.fd);
}

// Helper function to load string entries
int load_string_entry(RDBLoader *loader, redis_db_t *db)
{
    // Read key
    uint32_t key_len = rdb_load_len(loader);
    char *temp_key = malloc(key_len + 1);
    if (!temp_key) {
        return -1;
    }

    read(loader->fd, temp_key, key_len);
    temp_key[key_len] = '\0';

    // Check first byte to see if it's encoded integer or string
    unsigned char first_byte;
    if (read(loader->fd, &first_byte, 1) != 1) {
        free(temp_key);
        return -1;
    }

    redis_object_t *obj;
    
    if (first_byte == RDB_ENC_INT8) { // 0xF0 - INT8 encoding
        int8_t intval;
        read(loader->fd, &intval, 1);
        
        // Create number object
        char int_str[32];
        snprintf(int_str, sizeof(int_str), "%d", intval);
        obj = redis_object_create_number(int_str);
        
        printf("DB %d: Key='%s' → Value=%d (NUMBER)\n", loader->dbnum, temp_key, intval);
    }
    else {
        // It's a string length byte - seek back and read as length
        lseek(loader->fd, -1, SEEK_CUR);
        
        uint32_t val_len = rdb_load_len(loader);
        char *temp_val = malloc(val_len + 1);
        if (!temp_val) {
            free(temp_key);
            return -1;
        }

        read(loader->fd, temp_val, val_len);
        temp_val[val_len] = '\0';
        
        // Create string object
        obj = redis_object_create_string(temp_val);
        
        printf("DB %d: Key='%s' → Value='%s' (STRING)\n", loader->dbnum, temp_key, temp_val);
        free(temp_val);
    }

    if (obj) {
        hash_table_set(db->dict, temp_key, obj);
    }

    free(temp_key);
    return obj ? 0 : -1;
}

// Helper function to load list entries
int load_list_entry(RDBLoader *loader, redis_db_t *db)
{
    // Read key
    uint32_t key_len = rdb_load_len(loader);
    char *temp_key = malloc(key_len + 1);
    if (!temp_key) {
        return -1;
    }

    read(loader->fd, temp_key, key_len);
    temp_key[key_len] = '\0';

    // Read list length
    uint32_t list_length = rdb_load_len(loader);
    
    // Create list object
    redis_object_t *list_obj = redis_object_create_list();
    if (!list_obj) {
        free(temp_key);
        return -1;
    }

    redis_list_t *list = (redis_list_t*)list_obj->ptr;
    
    printf("DB %d: Loading LIST Key='%s' with %u elements\n", loader->dbnum, temp_key, list_length);

    // Load each list element
    for (uint32_t i = 0; i < list_length; i++) {
        uint32_t elem_len = rdb_load_len(loader);
        char *element = malloc(elem_len + 1);
        if (!element) {
            redis_object_destroy(list_obj);
            free(temp_key);
            return -1;
        }

        read(loader->fd, element, elem_len);
        element[elem_len] = '\0';

        list_rpush(list, element);
        
        printf("  Element %u: '%s'\n", i, element);
        free(element);
    }

    hash_table_set(db->dict, temp_key, list_obj);
    printf("DB %d: Successfully loaded LIST '%s' with %zu elements\n", 
           loader->dbnum, temp_key, list->length);

    free(temp_key);
    return 0;
}

int load_stream_entry_full(RDBLoader *loader, redis_db_t *db)
{
    // Read key
    uint32_t key_len = rdb_load_len(loader);
    char *temp_key = malloc(key_len + 1);
    if (!temp_key) {
        return -1;
    }

    read(loader->fd, temp_key, key_len);
    temp_key[key_len] = '\0';

    // Load stream using your existing load_stream function
    redis_stream_t *stream = load_stream(loader);
    if (!stream) {
        free(temp_key);
        return -1;
    }

    // Create stream object using your existing function
    redis_object_t *stream_obj = redis_object_create_stream(stream);
    if (!stream_obj) {
        redis_stream_destroy(stream);
        free(temp_key);
        return -1;
    }

    // Add to database
    hash_table_set(db->dict, temp_key, stream_obj);

    printf("DB %d: Successfully loaded STREAM '%s' with %zu entries, Last ID='%s'\n", 
           loader->dbnum, temp_key, stream->length, 
           stream->last_id ? stream->last_id : "NULL");

    if (stream->length > 0) {
        printf("  Stream entries:\n");
        void **results = NULL;
        int entry_count = 0;
        
        radix_tree_range(stream->entries_tree, "-", "+", &results, &entry_count);
        
        int print_count = (entry_count < 3) ? entry_count : 3;
        for (int i = 0; i < print_count; i++) {
            stream_entry_t *entry = (stream_entry_t*)results[i];
            printf("    ID: %s, Fields: ", entry->id);
            for (size_t j = 0; j < entry->field_count; j++) {
                printf("%s=%s", entry->fields[j].name, entry->fields[j].value);
                if (j < entry->field_count - 1) printf(", ");
            }
            printf("\n");
        }
        if (entry_count > 3) printf("    ... (%d more entries)\n", entry_count - 3);
        
        free(results);
    }

    free(temp_key);
    return 0;
}

uint32_t rdb_load_len(RDBLoader *loader)
{
    unsigned char buf[5];
    if (read(loader->fd, buf, 1) != 1)
        return 0;

    if ((buf[0] & 0xC0) == 0)
    { // 6-bit len
        return buf[0] & 0x3F;
    }
    else if ((buf[0] & 0xC0) == 0x40)
    { // 14-bit len
        uint8_t byte2;
        read(loader->fd, &byte2, 1);
        return ((buf[0] & 0x3F) << 8) | byte2;
    }
    else if (buf[0] == 0x80)
    { // 32-bit len
        uint32_t len;
        read(loader->fd, &len, 4);
        return ntohl(len);
    }
    return 0;
}

int rdb_save_database(io_buffer *rdb, redis_db_t *db)
{
    if (!rdb || !db || !db->dict)
        return -1;

    // 1. Write RDB header (magic + version)
    const char *magic = "REDIS0009"; // REDIS + version 0009
    buffer_write(rdb, magic, 9);

    // 2. Write database selector
    uint8_t db_selector = 0xFE;
    buffer_write(rdb, &db_selector, 1);
    uint8_t db_num = 1;
    buffer_write(rdb, &db_num, 1);

    // 3. Write all key-value pairs
    hash_table_iterator_t *iter = hash_table_iterator_create(db->dict);
    if (!iter)
        return -1;

    char *key;
    void *value;

    while (hash_table_iterator_next(iter, &key, &value))
    {
        redis_object_t *obj = (redis_object_t *)value;
        sds key_sds = sdsnew(key);
        save_key_value(rdb, key_sds, obj);
        sdsfree(key_sds);
    }

    // Write EOF marker
    uint8_t eof = 0xFF;
    buffer_write(rdb, &eof, 1);

    hash_table_iterator_destroy(iter);
    return 0;
}
redis_stream_t *load_stream(RDBLoader *loader)
{
    // Load stream length
    uint32_t stream_length = rdb_load_len(loader);
    
    // Load last_id
    uint32_t last_id_len = rdb_load_len(loader);
    char *last_id = NULL;
    if (last_id_len > 0) {
        last_id = malloc(last_id_len + 1);
        if (!last_id) return NULL;
        read(loader->fd, last_id, last_id_len);
        last_id[last_id_len] = '\0';
    }
    
    uint32_t max_len = rdb_load_len(loader);
    
    // Create stream
    redis_stream_t *stream = redis_stream_create();
    if (!stream) {
        free(last_id);
        return NULL;
    }
    
    // Set stream properties
    stream->max_len = max_len;
    if (last_id && strlen(last_id) > 0) {
        stream->last_id = strdup(last_id);
        // Parse last_id to set timestamp and sequence
        uint64_t timestamp, sequence;
        if (parse_stream_id(last_id, &timestamp, &sequence) == 0) {
            stream->last_timestamp_ms = timestamp;
            stream->last_sequence = sequence;
        }
    }
    free(last_id);
    
    // Load each entry
    for (uint32_t i = 0; i < stream_length; i++) {
        if (load_stream_entry(loader, stream) == -1) {
            redis_stream_destroy(stream);
            return NULL;
        }
    }
    
    return stream;
}

int load_stream_entry(RDBLoader *loader, redis_stream_t *stream)
{
    // Load entry ID
    uint32_t id_len = rdb_load_len(loader);
    char *entry_id = malloc(id_len + 1);
    if (!entry_id) return -1;
    
    read(loader->fd, entry_id, id_len);
    entry_id[id_len] = '\0';
    
    // Load field count
    uint32_t field_count = rdb_load_len(loader);
    
    // Allocate arrays for field names and values
    char **field_names = malloc(field_count * sizeof(char*));
    char **field_values = malloc(field_count * sizeof(char*));
    
    if (!field_names || !field_values) {
        free(entry_id);
        free(field_names);
        free(field_values);
        return -1;
    }
    
    // Load each field-value pair
    for (uint32_t j = 0; j < field_count; j++) {
        // Load field name
        uint32_t name_len = rdb_load_len(loader);
        field_names[j] = malloc(name_len + 1);
        if (!field_names[j]) {
            // Cleanup on error
            for (uint32_t k = 0; k < j; k++) {
                free(field_names[k]);
                free(field_values[k]);
            }
            free(field_names);
            free(field_values);
            free(entry_id);
            return -1;
        }
        read(loader->fd, field_names[j], name_len);
        field_names[j][name_len] = '\0';
        
        // Load field value
        uint32_t value_len = rdb_load_len(loader);
        field_values[j] = malloc(value_len + 1);
        if (!field_values[j]) {
            // Cleanup on error
            free(field_names[j]);
            for (uint32_t k = 0; k < j; k++) {
                free(field_names[k]);
                free(field_values[k]);
            }
            free(field_names);
            free(field_values);
            free(entry_id);
            return -1;
        }
        read(loader->fd, field_values[j], value_len);
        field_values[j][value_len] = '\0';
    }
    
    // Create and insert entry
    stream_entry_t *entry = stream_entry_create(entry_id, 
                                                (const char**)field_names,
                                                (const char**)field_values,
                                                field_count);
    
    if (entry) {
        radix_tree_insert(stream->entries_tree, entry_id, strlen(entry_id), entry);
        stream->length++;
    }
    
    for (uint32_t j = 0; j < field_count; j++) {
        free(field_names[j]);
        free(field_values[j]);
    }
    free(field_names);
    free(field_values);
    free(entry_id);
    
    return entry ? 0 : -1;
}