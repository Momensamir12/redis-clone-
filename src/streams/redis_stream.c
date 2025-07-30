#include "redis_stream.h"
#include "../lib/radix_tree.h"  // Include here
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

// Get current timestamp in milliseconds
static uint64_t get_current_timestamp_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

redis_stream_t *redis_stream_create(void) {
    redis_stream_t *stream = calloc(1, sizeof(redis_stream_t));
    if (!stream) return NULL;
    
    // Create radix tree to store entries by ID
    stream->entries_tree = radix_tree_create();
    if (!stream->entries_tree) {
        free(stream);
        return NULL;
    }
    
    stream->last_timestamp_ms = 0;
    stream->last_sequence = 0;
    stream->length = 0;
    stream->max_len = 0;  // Unlimited
    
    return stream;
}

void redis_stream_destroy(redis_stream_t *stream) {
    if (!stream) return;
    
    // TODO: We need to iterate through radix tree and free all entries
    // For now, this will leak the entry data
    if (stream->entries_tree) {
        radix_tree_destroy(stream->entries_tree);
    }
    
    free(stream->last_id);
    free(stream);
}

stream_entry_t *stream_entry_create(const char *id, const char **field_names, 
                                   const char **values, size_t field_count) {
    if (!id || field_count == 0) return NULL;
    
    stream_entry_t *entry = calloc(1, sizeof(stream_entry_t));
    if (!entry) return NULL;
    
    entry->id = strdup(id);
    if (!entry->id) {
        free(entry);
        return NULL;
    }
    
    // Parse ID to extract timestamp and sequence
    char *dash = strchr(id, '-');
    if (dash) {
        entry->timestamp_ms = strtoull(id, NULL, 10);
        entry->sequence = strtoull(dash + 1, NULL, 10);
    }
    
    // Create fields array
    entry->fields = calloc(field_count, sizeof(stream_field_t));
    if (!entry->fields) {
        free(entry->id);
        free(entry);
        return NULL;
    }
    
    for (size_t i = 0; i < field_count; i++) {
        entry->fields[i].name = strdup(field_names[i]);
        entry->fields[i].value = strdup(values[i]);
        
        if (!entry->fields[i].name || !entry->fields[i].value) {
            // Cleanup on error
            for (size_t j = 0; j <= i; j++) {
                free(entry->fields[j].name);
                free(entry->fields[j].value);
            }
            free(entry->fields);
            free(entry->id);
            free(entry);
            return NULL;
        }
    }
    entry->field_count = field_count;
    
    return entry;
}

void stream_entry_destroy(stream_entry_t *entry) {
    if (!entry) return;
    
    free(entry->id);
    for (size_t i = 0; i < entry->field_count; i++) {
        free(entry->fields[i].name);
        free(entry->fields[i].value);
    }
    free(entry->fields);
    free(entry);
}

static char *generate_stream_id(redis_stream_t *stream, const char *id_hint) {
    uint64_t timestamp_ms;
    uint64_t sequence;
    
    if (id_hint && strcmp(id_hint, "*") != 0) {
        // Parse provided ID
        char *dash = strchr(id_hint, '-');
        if (!dash) return NULL;
        
        timestamp_ms = strtoull(id_hint, NULL, 10);
        sequence = strtoull(dash + 1, NULL, 10);
        
        // Validate that it's greater than last ID
        if (timestamp_ms < stream->last_timestamp_ms || 
            (timestamp_ms == stream->last_timestamp_ms && sequence <= stream->last_sequence)) {
            return NULL;  // ID must be greater than last
        }
    } else {
        // Auto-generate ID
        timestamp_ms = get_current_timestamp_ms();
        
        if (timestamp_ms == stream->last_timestamp_ms) {
            sequence = stream->last_sequence + 1;
        } else if (timestamp_ms > stream->last_timestamp_ms) {
            sequence = 0;
        } else {
            // Clock went backwards, use last timestamp
            timestamp_ms = stream->last_timestamp_ms;
            sequence = stream->last_sequence + 1;
        }
    }
    
    // Update stream's last ID info
    stream->last_timestamp_ms = timestamp_ms;
    stream->last_sequence = sequence;
    
    // Generate ID string
    char *id = malloc(32);
    if (!id) return NULL;
    
    snprintf(id, 32, "%llu-%llu", 
             (unsigned long long)timestamp_ms, 
             (unsigned long long)sequence);
    
    free(stream->last_id);
    stream->last_id = strdup(id);
    
    return id;
}

char *redis_stream_add(redis_stream_t *stream, const char *id, 
                       const char **field_names, const char **values, size_t field_count) {
    if (!stream || field_count == 0) return NULL;
    
    // Generate or validate ID
    char *entry_id = generate_stream_id(stream, id);
    if (!entry_id) return NULL;
    
    // Create entry
    stream_entry_t *entry = stream_entry_create(entry_id, field_names, values, field_count);
    if (!entry) {
        free(entry_id);
        return NULL;
    }
    
    // Add to radix tree
    radix_tree_insert(stream->entries_tree, entry_id, strlen(entry_id), entry);
    stream->length++;
    
    return entry_id;  // Caller should free this
}

stream_entry_t *redis_stream_get(redis_stream_t *stream, const char *id) {
    if (!stream || !id) return NULL;
    
    return (stream_entry_t *)radix_search(stream->entries_tree, (char *)id, strlen(id));
}

size_t redis_stream_len(redis_stream_t *stream) {
    return stream ? stream->length : 0;
}