#ifndef RADIX_TREE_H
#define RADIX_TREE_H

#include <stddef.h>
#include <stdint.h>

typedef struct radix_node {
    char *key;
    size_t key_len;
    void *data;
    struct radix_node **children;  // NULL until needed
    size_t children_count;
    size_t children_capacity;
    uint8_t is_compressed;  // For Redis-style compressed nodes
} radix_node_t;

typedef struct radix_tree {
    radix_node_t *root;
    size_t size;               // Number of keys in the tree
} radix_tree_t;

// Core functions
radix_node_t *radix_node_create(char *key, size_t key_len, void *data);
radix_tree_t *radix_tree_create(void);
void radix_tree_insert(radix_tree_t *tree, char *key, size_t key_len, void *data);
void *radix_search(radix_tree_t *tree, char *key, size_t key_len);
void radix_tree_destroy(radix_tree_t *tree);

// Utility functions
void radix_tree_print(radix_tree_t *tree);
size_t radix_tree_size(radix_tree_t *tree);
size_t common_prefix_len(char *c1, char *c2, size_t len1, size_t len2);
void radix_tree_range(radix_tree_t *tree, char *start, char *end, char ***results, int *count);
radix_node_t *find_child_with_prefix(radix_node_t *node, char *key, size_t key_len, size_t *common_prefix);

#endif // RADIX_TREE_H