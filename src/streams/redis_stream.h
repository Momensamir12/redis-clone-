#ifndef REDIS_STREAM_H
#define REDIS_STREAM_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

// Forward declaration - we don't need to include radix_tree.h here
typedef struct radix_tree radix_tree_t;

// Stream entry field-value pair
typedef struct stream_field {
    char *name;
    char *value;
} stream_field_t;

// Individual stream entry
typedef struct stream_entry {
    char *id;                    // Stream ID (e.g., "1234567890123-0")
    uint64_t timestamp_ms;       // Millisecond timestamp
    uint64_t sequence;           // Sequence number
    stream_field_t *fields;      // Array of field-value pairs
    size_t field_count;          // Number of fields
} stream_entry_t;

// Stream structure - ONE stream with multiple entries
typedef struct redis_stream {
    radix_tree_t *entries_tree;  // Tree of entries (ID -> entry)
    char *last_id;               // Last generated ID
    uint64_t last_timestamp_ms;  // Last timestamp used
    uint64_t last_sequence;      // Last sequence used
    size_t length;               // Number of entries in this stream
    size_t max_len;              // Maximum length (0 = unlimited)
} redis_stream_t;

// Stream functions
redis_stream_t *redis_stream_create(void);
void redis_stream_destroy(redis_stream_t *stream);

// Entry operations for THIS stream
char *redis_stream_add(redis_stream_t *stream, const char *id, 
                       const char **field_names, const char **values, 
                       size_t field_count, int *error_code);
stream_entry_t *redis_stream_get(redis_stream_t *stream, const char *id);
int redis_stream_delete(redis_stream_t *stream, const char *id);
size_t redis_stream_len(redis_stream_t *stream);

// Range queries within THIS stream
typedef struct stream_range_result {
    stream_entry_t **entries;
    size_t count;
} stream_range_result_t;

stream_range_result_t *redis_stream_range(redis_stream_t *stream, 
                                         const char *start, const char *end, 
                                         size_t count);
void stream_range_result_destroy(stream_range_result_t *result);

// Utility functions
stream_entry_t *stream_entry_create(const char *id, const char **field_names, 
                                   const char **values, size_t field_count);
void stream_entry_destroy(stream_entry_t *entry);
int get_next_stream_id(const char *current_id, char *next_id, size_t buffer_size);
int parse_stream_id(const char *id_str, uint64_t *timestamp, uint64_t *sequence);

#endif // REDIS_STREAM_H