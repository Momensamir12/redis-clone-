// redis_sorted_set.h
#ifndef SORTED_SET_H
#define SORTED_SET_H

#include <stddef.h>
#include <stdbool.h>
#include "../hash_table/hash_table.h"

#define SKIPLIST_MAXLEVEL 32
#define SKIPLIST_P 0.25

// Forward declarations
typedef struct skip_list_node skip_list_node_t;
typedef struct skip_list skip_list_t;
typedef struct redis_sorted_set redis_sorted_set_t;

// Skip list node
struct skip_list_node {
    double score;
    char *member;
    skip_list_node_t **forward;  // Array of forward pointers
    int level;
};

// Skip list structure
struct skip_list {
    skip_list_node_t *header;
    int level;          // Current max level
    size_t length;      // Number of nodes
};

// Sorted set member for range operations
typedef struct {
    char *member;
    double score;
} sorted_set_member_t;

// Redis sorted set structure
struct redis_sorted_set {
    skip_list_t *skiplist;    // Skip list for ordered access
    hash_table_t *dict;       // Hash table for O(1) lookups
};

// Function declarations
redis_sorted_set_t *redis_sorted_set_create(void);
void redis_sorted_set_destroy(redis_sorted_set_t *zset);
int sorted_set_add(redis_sorted_set_t *zset, const char *member, double score);
int sorted_set_remove(redis_sorted_set_t *zset, const char *member);
int sorted_set_score(redis_sorted_set_t *zset, const char *member, double *score);
size_t sorted_set_card(redis_sorted_set_t *zset);
int sorted_set_range(redis_sorted_set_t *zset, int start, int stop, sorted_set_member_t **members);

// Skip list internal functions
skip_list_t *skiplist_create(void);
void skiplist_destroy(skip_list_t *sl);
skip_list_node_t *skiplist_insert(skip_list_t *sl, double score, const char *member);
int skiplist_delete(skip_list_t *sl, double score, const char *member);
skip_list_node_t *skiplist_find(skip_list_t *sl, double score, const char *member);
long long sorted_set_rank(redis_sorted_set_t *zset, const char *member, double score);

#endif // REDIS_SORTED_SET_H