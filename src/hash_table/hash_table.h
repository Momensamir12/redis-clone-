#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct hash_entry{
    char *key;
    void *value;
    struct hash_entry *next;
}hash_entry_t;

typedef struct hash_table {
    hash_entry_t **buckets;
    size_t size;
    size_t count;
}hash_table_t;

typedef struct hash_table_iterator {
    hash_table_t *ht;
    size_t bucket_idx;
    hash_entry_t *current;
    hash_entry_t *next;  // For safe iteration during modifications
} hash_table_iterator_t;


hash_table_t *hash_table_create (size_t size);
void hash_table_set (hash_table_t *ht, char *key, void *value);
void *hash_table_get(hash_table_t *ht, const char *key);
void hash_table_delete(hash_table_t *ht, const char *key);
void hash_table_destroy(hash_table_t *ht);
// Iterator function declarations
hash_table_iterator_t *hash_table_iterator_create(hash_table_t *ht);
int hash_table_iterator_next(hash_table_iterator_t *iter, char **key, void **value);
void hash_table_iterator_destroy(hash_table_iterator_t *iter);

#endif