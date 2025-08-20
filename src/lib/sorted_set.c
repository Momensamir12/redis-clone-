
#include "sorted_set.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

// Random level generator for skip list
static int random_level(void) {
    static int first_call = 1;
    if (first_call) {
        srand((unsigned int)time(NULL));
        first_call = 0;
    }
    
    int level = 1;
    while (((double)rand() / RAND_MAX) < SKIPLIST_P && level < SKIPLIST_MAXLEVEL) {
        level++;
    }
    return level;
}

// Create skip list node
static skip_list_node_t *skiplist_node_create(int level, double score, const char *member) {
    skip_list_node_t *node = malloc(sizeof(skip_list_node_t));
    if (!node) return NULL;
    
    node->forward = malloc(sizeof(skip_list_node_t *) * level);
    if (!node->forward) {
        free(node);
        return NULL;
    }
    
    node->score = score;
    node->member = strdup(member);
    if (!node->member) {
        free(node->forward);
        free(node);
        return NULL;
    }
    
    node->level = level;
    
    // Initialize forward pointers to NULL
    for (int i = 0; i < level; i++) {
        node->forward[i] = NULL;
    }
    
    return node;
}

// Destroy skip list node
static void skiplist_node_destroy(skip_list_node_t *node) {
    if (!node) return;
    
    free(node->member);
    free(node->forward);
    free(node);
}

// Create skip list
skip_list_t *skiplist_create(void) {
    skip_list_t *sl = malloc(sizeof(skip_list_t));
    if (!sl) return NULL;
    
    // Create header node with maximum level
    sl->header = skiplist_node_create(SKIPLIST_MAXLEVEL, 0.0, "");
    if (!sl->header) {
        free(sl);
        return NULL;
    }
    
    sl->level = 1;
    sl->length = 0;
    
    return sl;
}

// Destroy skip list
void skiplist_destroy(skip_list_t *sl) {
    if (!sl) return;
    
    skip_list_node_t *current = sl->header->forward[0];
    
    while (current) {
        skip_list_node_t *next = current->forward[0];
        skiplist_node_destroy(current);
        current = next;
    }
    
    skiplist_node_destroy(sl->header);
    free(sl);
}

// Compare function for skip list ordering
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
static int skiplist_compare(double score1, const char *member1, double score2, const char *member2) {
    if (score1 < score2) return -1;
    if (score1 > score2) return 1;
    
    // Same score, compare by member lexicographically
    int cmp = strcmp(member1, member2);
    if (cmp < 0) return -1;
    if (cmp > 0) return 1;
    return 0;
}

// Insert node into skip list
skip_list_node_t *skiplist_insert(skip_list_t *sl, double score, const char *member) {
    skip_list_node_t *update[SKIPLIST_MAXLEVEL];
    skip_list_node_t *current = sl->header;
    
    // Find insertion point
    for (int i = sl->level - 1; i >= 0; i--) {
        while (current->forward[i] && 
               skiplist_compare(current->forward[i]->score, current->forward[i]->member, score, member) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    // Check if member already exists
    current = current->forward[0];
    if (current && skiplist_compare(current->score, current->member, score, member) == 0) {
        // Update existing node's score
        current->score = score;
        return current;
    }
    
    // Create new node
    int level = random_level();
    skip_list_node_t *new_node = skiplist_node_create(level, score, member);
    if (!new_node) return NULL;
    
    // Update skip list level if necessary
    if (level > sl->level) {
        for (int i = sl->level; i < level; i++) {
            update[i] = sl->header;
        }
        sl->level = level;
    }
    
    // Insert node
    for (int i = 0; i < level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }
    
    sl->length++;
    return new_node;
}

// Find node in skip list
skip_list_node_t *skiplist_find(skip_list_t *sl, double score, const char *member) {
    skip_list_node_t *current = sl->header;
    
    for (int i = sl->level - 1; i >= 0; i--) {
        while (current->forward[i] && 
               skiplist_compare(current->forward[i]->score, current->forward[i]->member, score, member) < 0) {
            current = current->forward[i];
        }
    }
    
    current = current->forward[0];
    if (current && skiplist_compare(current->score, current->member, score, member) == 0) {
        return current;
    }
    
    return NULL;
}

// Delete node from skip list
int skiplist_delete(skip_list_t *sl, double score, const char *member) {
    skip_list_node_t *update[SKIPLIST_MAXLEVEL];
    skip_list_node_t *current = sl->header;
    
    // Find node to delete
    for (int i = sl->level - 1; i >= 0; i--) {
        while (current->forward[i] && 
               skiplist_compare(current->forward[i]->score, current->forward[i]->member, score, member) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0];
    if (!current || skiplist_compare(current->score, current->member, score, member) != 0) {
        return 0; // Node not found
    }
    
    // Remove node from all levels
    for (int i = 0; i < sl->level; i++) {
        if (update[i]->forward[i] != current) break;
        update[i]->forward[i] = current->forward[i];
    }
    
    skiplist_node_destroy(current);
    
    // Update skip list level
    while (sl->level > 1 && sl->header->forward[sl->level - 1] == NULL) {
        sl->level--;
    }
    
    sl->length--;
    return 1;
}

// Create redis sorted set
redis_sorted_set_t *redis_sorted_set_create(void) {
    redis_sorted_set_t *zset = malloc(sizeof(redis_sorted_set_t));
    if (!zset) return NULL;
    
    zset->skiplist = skiplist_create();
    if (!zset->skiplist) {
        free(zset);
        return NULL;
    }
    
    zset->dict = hash_table_create(16);
    if (!zset->dict) {
        skiplist_destroy(zset->skiplist);
        free(zset);
        return NULL;
    }
    
    return zset;
}

// Destroy redis sorted set
void redis_sorted_set_destroy(redis_sorted_set_t *zset) {
    if (!zset) return;
    
    skiplist_destroy(zset->skiplist);
    hash_table_destroy(zset->dict);
    free(zset);
}

// Add member to sorted set
int sorted_set_add(redis_sorted_set_t *zset, const char *member, double score) {
    if (!zset || !member) return -1;
    
    // Check if member already exists
    double *existing_score = (double *)hash_table_get(zset->dict, member);
    bool is_new = (existing_score == NULL);
    
    if (existing_score) {
        // Remove from skip list with old score
        skiplist_delete(zset->skiplist, *existing_score, member);
        free(existing_score);
    }
    
    // Insert with new score
    skip_list_node_t *node = skiplist_insert(zset->skiplist, score, member);
    if (!node) return -1;
    
    // Update hash table
    double *score_ptr = malloc(sizeof(double));
    if (!score_ptr) {
        if (is_new) {
            skiplist_delete(zset->skiplist, score, member);
        }
        return -1;
    }
    *score_ptr = score;
    
    hash_table_set(zset->dict, strdup(member), score_ptr);
    
    return is_new ? 1 : 0; // Return 1 for new member, 0 for updated
}

// Remove member from sorted set
int sorted_set_remove(redis_sorted_set_t *zset, const char *member) {
    if (!zset || !member) return 0;
    
    double *score = (double *)hash_table_get(zset->dict, member);
    if (!score) return 0;
    
    // Remove from skip list
    int result = skiplist_delete(zset->skiplist, *score, member);
    if (result) {
        // Remove from hash table
        hash_table_delete(zset->dict, member);
        free(score);
    }
    
    return result;
}

// Get score of member
int sorted_set_score(redis_sorted_set_t *zset, const char *member, double *score) {
    if (!zset || !member || !score) return 0;
    
    double *score_ptr = (double *)hash_table_get(zset->dict, member);
    if (!score_ptr) return 0;
    
    *score = *score_ptr;
    return 1;
}

// Get cardinality of sorted set
size_t sorted_set_card(redis_sorted_set_t *zset) {
    if (!zset) return 0;
    return zset->skiplist->length;
}

// Get range of members
int sorted_set_range(redis_sorted_set_t *zset, int start, int stop, sorted_set_member_t **members) {
    if (!zset || !members) return 0;
    
    size_t length = zset->skiplist->length;
    if (length == 0) return 0;
    
    // Handle negative indices
    if (start < 0) start += length;
    if (stop < 0) stop += length;
    
    // Clamp to valid range
    if (start < 0) start = 0;
    if (stop >= (int)length) stop = length - 1;
    if (start > stop) return 0;
    
    int count = stop - start + 1;
    *members = malloc(sizeof(sorted_set_member_t) * count);
    if (!*members) return 0;
    
    // Navigate to start position
    skip_list_node_t *current = zset->skiplist->header->forward[0];
    for (int i = 0; i < start && current; i++) {
        current = current->forward[0];
    }
    
    // Collect members
    for (int i = 0; i < count && current; i++) {
        (*members)[i].member = current->member;
        (*members)[i].score = current->score;
        current = current->forward[0];
    }
    
    return count;
}
long long sorted_set_rank(redis_sorted_set_t *zset, const char *member, double score) {
    if (!zset || !member) return -1;
    
    skip_list_node_t *current = zset->skiplist->header->forward[0];
    long long rank = 0;
    
    while (current) {
        int cmp = skiplist_compare(current->score, current->member, score, member);
        
        if (cmp == 0) {
            return rank;
        } else if (cmp < 0) {
            rank++;
            current = current->forward[0];
        } else {
            return -1;
        }
    }
    
    return -1; 
}