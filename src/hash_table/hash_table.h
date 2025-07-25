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
    hash_entry **buckets;
    size_t size;
    size_t count;
}hash_table_t;

hash_table *hash_table_create (size_t size);
void hash_table_set (hash_table_t ht, char *key, void *value);
void *hash_table_get(hash_table_t *ht, const char *key);
void hash_table_delete(hash_table_t *ht, const char *key);
#endif