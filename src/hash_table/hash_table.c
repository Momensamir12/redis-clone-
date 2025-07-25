#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static size_t hash(const char *key, size_t size)
{
    size_t hash = 5381;
    int c;
    while ((c = *key++))
    {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % size;
}

hash_table_t *hash_table_create(size_t size)
{
    hash_table_t *ht = calloc(1, sizeof(hash_table_t));
    if (!ht)
        return NULL;
    ht->size = size;
    ht->count = 0;
    ht->buckets = calloc(size, sizeof(hash_entry_t));
    if (!ht->buckets)
    {
        free(ht);
        return NULL;
    }

    return ht;
}

void hash_table_set (hash_table_t *ht, char *key, void *value)
{
    size_t index = hash(key, ht->size);
    hash_entry_t *entry = ht->buckets[index];
    while(entry){
        if(entry->key == key){
            entry->value = value;
            return;
        }
        entry = entry->next;
    }
    hash_entry_t *new_entry = malloc(sizeof(hash_entry_t));
    new_entry->key = strdup(key);
    new_entry->value = value;
    new_entry->next = ht->buckets[index];
    ht->buckets[index] = new_entry;
    ht->count++;
}

void *hash_table_get(hash_table_t *ht, const char *key)
{
    size_t index = hash(key, ht->size);
    hash_entry_t * entry = ht->buckets[index];
    while(entry)
    {
        if(strcmp(entry->key, key) == 0){
            return entry->value;
        }
        entry = entry->next;
    }
    return NULL;
}

void hash_table_delete(hash_table_t *ht, const char *key)
{
    size_t index = hash(key, ht->size);
    hash_entry_t * entry = ht->buckets[index];
    hash_entry_t * prev = NULL;
    while(entry){
        if(strcmp(entry->key, key) == 0){
           if(prev){
            prev->next = entry->next;
           }
           else{
            ht->buckets[index] = entry->next;
           }
           free(entry);
           free(entry->key);
           ht->count--;
        }
        prev = entry;
        entry = entry->next;
    }
}

void hash_table_destroy(hash_table_t *ht) {
    if (!ht) return;
    
    // Free all entries
    for (size_t i = 0; i < ht->size; i++) {
        hash_entry_t *entry = ht->buckets[i];
        while (entry) {
            hash_entry_t *next = entry->next;
            free(entry->key);
            free(entry);
            entry = next;
        }
    }
    
    free(ht->buckets);
    free(ht);
}