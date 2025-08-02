#include "radix_tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void radix_tree_range_traverse(radix_node_t *node, char *start, char *end, 
                                     void ***results, int *count, int *capacity);

radix_node_t *radix_node_create(char *key, size_t key_len, void *data) {
    if (!key)
        return NULL;
    
    radix_node_t *node = calloc(1, sizeof(radix_node_t));
    if (!node)
        return NULL;
    
    if (key_len > 0) {
        node->key = malloc(key_len + 1);
        if (!node->key) {
            free(node);
            return NULL;
        }
        
        memcpy(node->key, key, key_len);
        node->key[key_len] = '\0';
        node->key_len = key_len;
    }
    
    if (data) {
        node->data = data;
    }
    
    return node;
}

radix_tree_t *radix_tree_create(void) {
    radix_tree_t *tree = calloc(1, sizeof(radix_tree_t));
    if (!tree)
        return NULL;

    tree->root = radix_node_create("", 0, NULL);
    if (!tree->root) {
        free(tree);
        return NULL;
    }

    return tree;
}

static int allocate_child(int number_of_children, radix_node_t *node) {
    if (!node || number_of_children <= 0)
        return -1;

    if (!node->children) {
        node->children_capacity = number_of_children > 4 ? number_of_children : 4;
        node->children = calloc(node->children_capacity, sizeof(radix_node_t *));
        if (!node->children)
            return -1;
        return 0;
    }
    
    if (node->children_capacity >= number_of_children)
        return 0;
    
    size_t new_capacity = node->children_capacity * 2;
    if (new_capacity < number_of_children)
        new_capacity = number_of_children;

    radix_node_t **children = realloc(node->children, new_capacity * sizeof(radix_node_t *));
    if (!children)
        return -1;
    
    // Zero out new slots
    for (size_t i = node->children_capacity; i < new_capacity; i++) {
        children[i] = NULL;
    }
    
    node->children = children;
    node->children_capacity = new_capacity;

    return 0;
}

size_t common_prefix_len(char *c1, char *c2, size_t len1, size_t len2) {
    size_t max = len1 < len2 ? len1 : len2;
    size_t i = 0;
    
    while (i < max) {
        if (c1[i] == c2[i])
            i++;
        else
            break;
    }
    
    return i;
}

radix_node_t *find_child_with_prefix(radix_node_t *node, char *key, size_t key_len, size_t *common_prefix) {
    if (!node || !key || !common_prefix)
        return NULL;
    
    *common_prefix = 0;
    
    for (size_t i = 0; i < node->children_count; i++) {
        radix_node_t *child = node->children[i];
        if (!child || !child->key) 
            continue;
        
        if (key[0] == child->key[0]) {
            *common_prefix = common_prefix_len(key, child->key, key_len, child->key_len);
            if (*common_prefix > 0)
                return child;
        }
    }
    
    return NULL;
}

static int add_child(radix_node_t *parent, radix_node_t *child) {
    if (!parent || !child)
        return -1;
    
    if (allocate_child(parent->children_count + 1, parent) < 0) {
        return -1;
    }
    
    parent->children[parent->children_count++] = child;
    return 0;
}

static void split_node(radix_node_t *node, size_t split_pos) {
    if (!node || split_pos >= node->key_len)
        return;
    
    // Create key for the remainder
    size_t remainder_len = node->key_len - split_pos;
    char *remainder_key = malloc(remainder_len + 1);
    if (!remainder_key)
        return;
    
    memcpy(remainder_key, node->key + split_pos, remainder_len);
    remainder_key[remainder_len] = '\0';
    
    // Create new child with remainder
    radix_node_t *new_child = radix_node_create(remainder_key, remainder_len, node->data);
    free(remainder_key);
    
    if (!new_child)
        return;
    
    // Transfer children to new child
    new_child->children = node->children;
    new_child->children_count = node->children_count;
    new_child->children_capacity = node->children_capacity;
    
    // Update current node
    node->key[split_pos] = '\0';
    node->key_len = split_pos;
    node->data = NULL;
    node->children = NULL;
    node->children_count = 0;
    node->children_capacity = 0;
    
    // Add new child to current node
    add_child(node, new_child);
}

void radix_tree_insert(radix_tree_t *tree, char *key, size_t key_len, void *data) {
    if (!tree || !key || key_len == 0)
        return;

    radix_node_t *cur = tree->root;
    if (!cur)
        return;
    
    size_t cur_pos = 0;
    
    while (cur_pos < key_len) {
        size_t common_prefix = 0;
        radix_node_t *child = find_child_with_prefix(cur, 
                                                     key + cur_pos,
                                                     key_len - cur_pos,
                                                     &common_prefix);

        if (!child) {
            // No matching child - create new node
            char *remaining_key = key + cur_pos;
            size_t remaining_len = key_len - cur_pos;
            
            radix_node_t *new_child = radix_node_create(remaining_key, remaining_len, data);
            if (new_child && add_child(cur, new_child) == 0) {
                tree->size++;
            }
            return;
        }
        
        // Partial match - need to split child
        if (common_prefix < child->key_len) {
            split_node(child, common_prefix);
            
            // Now add the diverging part
            size_t remaining = key_len - cur_pos - common_prefix;
            if (remaining > 0) {
                char *diverging_key = key + cur_pos + common_prefix;
                radix_node_t *new_child = radix_node_create(diverging_key, remaining, data);
                if (new_child && add_child(child, new_child) == 0) {
                    tree->size++;
                }
            } else {
                // Exact match after split
                if (!child->data) {
                    tree->size++;
                }
                child->data = data;
            }
            return;
        }
        
        // Full match of child's key - continue
        cur_pos += common_prefix;
        cur = child;
    }
    
    // Key exhausted - update current node
    if (!cur->data) {
        tree->size++;
    }
    cur->data = data;
}

void *radix_search(radix_tree_t *tree, char *key, size_t key_len) {
    if (!tree || !key || key_len == 0)
        return NULL;
    
    radix_node_t *cur = tree->root;
    size_t cur_pos = 0;
    
    while (cur_pos < key_len) {
        size_t common_prefix = 0;
        radix_node_t *child = find_child_with_prefix(cur, 
                                                     key + cur_pos, 
                                                     key_len - cur_pos, 
                                                     &common_prefix);
        if (!child)
            return NULL;
        
        if (common_prefix < child->key_len)
            return NULL;
        else if (common_prefix == child->key_len) {
            cur = child;
            cur_pos += common_prefix;
        }
    }
    
    if (cur_pos == key_len)
        return cur->data;
    
    return NULL;
}

void radix_tree_range(radix_tree_t *tree, char *start, char *end, void ***results, int *count)
{
    if (!tree || !tree->root || !results || !count) {
        *results = NULL;
        *count = 0;
        return;
    }
    
    // Handle special cases
    char *actual_start = start;
    char *actual_end = end;
    
    if (strcmp(start, "-") == 0) {
        actual_start = "0-0";
    }
    if (strcmp(end, "+") == 0) {
        actual_end = "999999999999999-999999999999999"; // Very large ID
    }
    
    // Initial capacity
    int capacity = 100;
    *results = malloc(capacity * sizeof(void*));
    *count = 0;
    
    // Traverse and collect
    radix_tree_range_traverse(tree->root, actual_start, actual_end, results, count, &capacity);
}

static void radix_tree_range_traverse(radix_node_t *node, char *start, char *end, 
                                     void ***results, int *count, int *capacity)
{
    if (!node) return;
    
    // If this node has data and key is in range
    if (node->data && node->key && node->key_len > 0) {
        if (strcmp(node->key, start) >= 0 && strcmp(node->key, end) <= 0) {
            // Expand array if needed
            if (*count >= *capacity) {
                *capacity *= 2;
                *results = realloc(*results, *capacity * sizeof(void*));
            }
            
            // Just store the raw data pointer
            (*results)[*count] = node->data;
            (*count)++;
        }
    }
    
    // Traverse children
    for (size_t i = 0; i < node->children_count; i++) {
        radix_tree_range_traverse(node->children[i], start, end, results, count, capacity);
    }
}

void radix_tree_print_node(radix_node_t *node, int depth) {
    if (!node) return;
    
    // Print indentation
    for (int i = 0; i < depth; i++) printf("  ");
    
    // Print node info
    printf("|-- ");
    if (node->key_len > 0) {
        printf("'%.*s'", (int)node->key_len, node->key);
    } else {
        printf("(root)");
    }
    
    if (node->data) {
        printf(" => %s", (char*)node->data);
    }
    printf(" [%zu children]\n", node->children_count);
    
    // Print children
    for (size_t i = 0; i < node->children_count; i++) {
        radix_tree_print_node(node->children[i], depth + 1);
    }
}

void radix_tree_print(radix_tree_t *tree) {
    if (!tree) {
        printf("Tree is NULL\n");
        return;
    }
    
    printf("Radix Tree (size: %zu):\n", tree->size);
    radix_tree_print_node(tree->root, 0);
}

size_t radix_tree_size(radix_tree_t *tree) {
    return tree ? tree->size : 0;
}

static void radix_node_destroy(radix_node_t *node) {
    if (!node) return;
    
    // Destroy all children first
    for (size_t i = 0; i < node->children_count; i++) {
        radix_node_destroy(node->children[i]);
    }
    
    // Free node resources
    free(node->key);
    free(node->children);
    free(node);
}

void radix_tree_destroy(radix_tree_t *tree) {
    if (!tree) return;
    
    radix_node_destroy(tree->root);
    free(tree);
}