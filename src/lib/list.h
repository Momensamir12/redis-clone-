#ifndef LIST_H
#define LIST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


typedef struct list_node {
    void *data;
    struct list_node *next;
    struct list_node *prev;
} list_node_t;

typedef struct redis_list {
    list_node_t *head;
    list_node_t *tail;
    size_t length;
} redis_list_t;

// Create/destroy
redis_list_t *list_create(void);
void list_destroy(redis_list_t *list);

// Redis operations
void list_lpush(redis_list_t *list, void *data);  // Add to left/head
void list_rpush(redis_list_t *list, void *data);  // Add to right/tail
void *list_lpop(redis_list_t *list);              // Remove from left
void *list_rpop(redis_list_t *list);              // Remove from right
size_t list_length(redis_list_t *list);           // Get length (LLEN)
void *list_index(redis_list_t *list, int index);  // Get by index (LINDEX)
char **list_range(redis_list_t *list, int start, int stop, int *count);

#endif