#include "redis_stream.h"
#include "../lib/radix_tree.h" 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static uint64_t get_current_timestamp_ms(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

redis_stream_t *redis_stream_create(void)
{
    redis_stream_t *stream = calloc(1, sizeof(redis_stream_t));
    if (!stream)
        return NULL;

    stream->entries_tree = radix_tree_create();
    if (!stream->entries_tree)
    {
        free(stream);
        return NULL;
    }

    stream->last_timestamp_ms = 0;
    stream->last_sequence = 0;
    stream->length = 0;
    stream->max_len = 0; 

    return stream;
}

void redis_stream_destroy(redis_stream_t *stream)
{
    if (!stream)
        return;

    if (stream->entries_tree)
    {
        void **entries = NULL;
        int count = 0;
        radix_tree_range(stream->entries_tree, "0-0", "999999999999999-999999999999999", 
                        &entries, &count);
        
        if (entries) {
            for (int i = 0; i < count; i++) {
                stream_entry_destroy((stream_entry_t *)entries[i]);
            }
            free(entries);
        }
        
        radix_tree_destroy(stream->entries_tree);
    }

    free(stream->last_id);
    free(stream);
}

int parse_stream_id(const char *id_str, uint64_t *timestamp, uint64_t *sequence)
{
    if (!id_str || !timestamp || !sequence)
        return -1;

    char *dash = strchr(id_str, '-');
    if (!dash)
        return -1;

    char *endptr;
    *timestamp = strtoull(id_str, &endptr, 10);
    if (endptr != dash)
        return -1; 

    *sequence = strtoull(dash + 1, &endptr, 10);
    if (*endptr != '\0')
        return -1; 

    return 0;
}


static int compare_stream_ids(const char *id1, const char *id2)
{
    uint64_t ts1, seq1, ts2, seq2;

    if (parse_stream_id(id1, &ts1, &seq1) != 0)
        return -2; // Invalid id1
    if (parse_stream_id(id2, &ts2, &seq2) != 0)
        return -2; // Invalid id2

    if (ts1 < ts2)
        return -1;
    if (ts1 > ts2)
        return 1;

    if (seq1 < seq2)
        return -1;
    if (seq1 > seq2)
        return 1;

    return 0; 
}


static int validate_explicit_id(const char *new_id, const char *last_id)
{
    if (strcmp(new_id, "0-0") == 0)
    {
        return -2; 
    }

    if (!last_id)
    {
        return compare_stream_ids(new_id, "0-0") > 0 ? 0 : -1;
    }

    return compare_stream_ids(new_id, last_id) > 0 ? 0 : -1;
}
stream_entry_t *stream_entry_create(const char *id, const char **field_names,
                                    const char **values, size_t field_count)
{
    if (!id || field_count == 0)
        return NULL;

    stream_entry_t *entry = calloc(1, sizeof(stream_entry_t));
    if (!entry)
        return NULL;

    entry->id = strdup(id);
    if (!entry->id)
    {
        free(entry);
        return NULL;
    }

    char *dash = strchr(id, '-');
    if (dash)
    {
        entry->timestamp_ms = strtoull(id, NULL, 10);
        entry->sequence = strtoull(dash + 1, NULL, 10);
    }

    entry->fields = calloc(field_count, sizeof(stream_field_t));
    if (!entry->fields)
    {
        free(entry->id);
        free(entry);
        return NULL;
    }

    for (size_t i = 0; i < field_count; i++)
    {
        entry->fields[i].name = strdup(field_names[i]);
        entry->fields[i].value = strdup(values[i]);

        if (!entry->fields[i].name || !entry->fields[i].value)
        {
            for (size_t j = 0; j <= i; j++)
            {
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

void stream_entry_destroy(stream_entry_t *entry)
{
    if (!entry)
        return;

    free(entry->id);
    for (size_t i = 0; i < entry->field_count; i++)
    {
        free(entry->fields[i].name);
        free(entry->fields[i].value);
    }
    free(entry->fields);
    free(entry);
}

static int is_partial_id_format(const char *id_str, uint64_t *timestamp)
{
    if (!id_str || !timestamp)
        return 0;

    char *dash = strchr(id_str, '-');
    if (!dash)
        return 0;

    if (strcmp(dash + 1, "*") != 0)
        return 0;

    char *endptr;
    *timestamp = strtoull(id_str, &endptr, 10);
    if (endptr != dash)
        return 0; 

    return 1; 
}

static uint64_t find_last_sequence_for_timestamp(redis_stream_t *stream, uint64_t timestamp)
{

    if (stream->last_id && stream->last_timestamp_ms == timestamp)
    {
        return stream->last_sequence;
    }

    return (timestamp == 0) ? 0 : -1;
}
static char *generate_stream_id(redis_stream_t *stream, const char *id_hint, int *error_code)
{
    uint64_t timestamp_ms;
    uint64_t sequence;

    *error_code = 0;

    if (id_hint && strcmp(id_hint, "*") != 0)
    {
        uint64_t partial_timestamp;
        if (is_partial_id_format(id_hint, &partial_timestamp))
        {
            timestamp_ms = partial_timestamp;

            uint64_t last_seq = find_last_sequence_for_timestamp(stream, timestamp_ms);

            if (last_seq == (uint64_t)-1)
            {
                if (timestamp_ms == 0)
                {
                    sequence = 1;
                }
                else
                {
                    sequence = 0;
                }
            }
            else
            {
                sequence = last_seq + 1;
            }

            char temp_id[32];
            snprintf(temp_id, sizeof(temp_id), "%llu-%llu",
                     (unsigned long long)timestamp_ms,
                     (unsigned long long)sequence);

            int validation_result = validate_explicit_id(temp_id, stream->last_id);
            if (validation_result != 0)
            {
                if (validation_result == -2)
                {
                    *error_code = 6;
                }
                else
                {
                    *error_code = 2;
                }
                return NULL;
            }
        }
        else
        {
            uint64_t provided_ts, provided_seq;
            if (parse_stream_id(id_hint, &provided_ts, &provided_seq) != 0)
            {
                *error_code = 1;
                return NULL;
            }

            int validation_result = validate_explicit_id(id_hint, stream->last_id);
            if (validation_result != 0)
            {
                if (validation_result == -2)
                {
                    *error_code = 6;
                }
                else
                {
                    *error_code = 2;
                }
                return NULL;
            }

            timestamp_ms = provided_ts;
            sequence = provided_seq;
        }
    }
    else
    {
        timestamp_ms = get_current_timestamp_ms();

        if (timestamp_ms == stream->last_timestamp_ms)
        {
            sequence = stream->last_sequence + 1;
        }
        else if (timestamp_ms > stream->last_timestamp_ms)
        {
            sequence = 0;
        }
        else
        {
            timestamp_ms = stream->last_timestamp_ms;
            sequence = stream->last_sequence + 1;
        }
    }

    stream->last_timestamp_ms = timestamp_ms;
    stream->last_sequence = sequence;

    char *id = malloc(32);
    if (!id)
    {
        *error_code = 3; 
        return NULL;
    }

    snprintf(id, 32, "%llu-%llu",
             (unsigned long long)timestamp_ms,
             (unsigned long long)sequence);

    free(stream->last_id);
    stream->last_id = strdup(id);

    return id;
}

char *redis_stream_add(redis_stream_t *stream, const char *id,
                       const char **field_names, const char **values,
                       size_t field_count, int *error_code)
{
    if (!stream || field_count == 0)
    {
        if (error_code)
            *error_code = 4; 
        return NULL;
    }

    int id_error = 0;
    char *entry_id = generate_stream_id(stream, id, &id_error);
    if (!entry_id)
    {
        if (error_code)
            *error_code = id_error;
        return NULL;
    }

    // Create entry
    stream_entry_t *entry = stream_entry_create(entry_id, field_names, values, field_count);
    if (!entry)
    {
        free(entry_id);
        if (error_code)
            *error_code = 5; 
    }

    radix_tree_insert(stream->entries_tree, entry_id, strlen(entry_id), entry);
    stream->length++;

    if (error_code)
        *error_code = 0; 
    return entry_id;
}

stream_entry_t *redis_stream_get(redis_stream_t *stream, const char *id)
{
    if (!stream || !id)
        return NULL;

    return (stream_entry_t *)radix_search(stream->entries_tree, (char *)id, strlen(id));
}

size_t redis_stream_len(redis_stream_t *stream)
{
    return stream ? stream->length : 0;
}

int get_next_stream_id(const char *current_id, char *next_id, size_t buffer_size)
{
    if (!current_id || !next_id) return -1;
    
    uint64_t timestamp, sequence;
    if (parse_stream_id(current_id, &timestamp, &sequence) != 0) {
        return -1;
    }
    
    if (sequence < UINT64_MAX) {
        sequence++;
    } else {
        if (timestamp < UINT64_MAX) {
            timestamp++;
            sequence = 0;
        } else {
            return -1;
        }
    }
    
    snprintf(next_id, buffer_size, "%llu-%llu", 
             (unsigned long long)timestamp, (unsigned long long)sequence);
    
    return 0;
}