#include <stdio.h>
#include <stdlib.h>
#include "list.h"

redis_list_t *list_create(void) {
    redis_list_t *list = malloc(sizeof(redis_list_t));
    if (!list) return NULL;
    
    list->head = NULL;
    list->tail = NULL;
    list->length = 0;
    return list;
}

void list_destroy(redis_list_t *list) {
    if (!list) return;
    
    list_node_t *current = list->head;
    while (current) {
        list_node_t *next = current->next;
        free(current);
        current = next;
    }
    free(list);
}

void list_lpush(redis_list_t *list, void *data) {
    list_node_t *node = malloc(sizeof(list_node_t));
    if (!node) return;
    
    node->data = data;
    node->prev = NULL;
    node->next = list->head;
    
    if (list->head) {
        list->head->prev = node;
    } else {
        list->tail = node;  // First element
    }
    
    list->head = node;
    list->length++;
}

void list_rpush(redis_list_t *list, void *data) {
    list_node_t *node = malloc(sizeof(list_node_t));
    if (!node) return;
    
    node->data = data;
    node->next = NULL;
    node->prev = list->tail;
    
    if (list->tail) {
        list->tail->next = node;
    } else {
        list->head = node;  // First element
    }
    
    list->tail = node;
    list->length++;
}

void *list_lpop(redis_list_t *list) {
    if (!list->head) return NULL;
    
    list_node_t *node = list->head;
    void *data = node->data;
    
    list->head = node->next;
    if (list->head) {
        list->head->prev = NULL;
    } else {
        list->tail = NULL;  // List is now empty
    }
    
    free(node);
    list->length--;
    return data;
}

void *list_rpop(redis_list_t *list) {
    if (!list->tail) return NULL;
    
    list_node_t *node = list->tail;
    void *data = node->data;
    
    list->tail = node->prev;
    if (list->tail) {
        list->tail->next = NULL;
    } else {
        list->head = NULL;  // List is now empty
    }
    
    free(node);
    list->length--;
    return data;
}

size_t list_length(redis_list_t *list) {
    return list->length;
}

void *list_index(redis_list_t *list, int index) {
    if (!list || list->length == 0) return NULL;
    
    // Handle negative indices (from end)
    if (index < 0) {
        index = list->length + index;
    }
    
    if (index < 0 || index >= list->length) return NULL;
    
    // Optimize: start from tail if index is closer to end
    if (index > list->length / 2) {
        list_node_t *current = list->tail;
        for (int i = list->length - 1; i > index; i--) {
            current = current->prev;
        }
        return current->data;
    } else {
        list_node_t *current = list->head;
        for (int i = 0; i < index; i++) {
            current = current->next;
        }
        return current->data;
    }
}

char **list_range(redis_list_t *list, int start, int stop, int *count) {
    if (!list || list->length == 0) {
        *count = 0;
        return NULL;
    }
    
    if (start < 0) start = list->length + start;
    if (stop < 0) stop = list->length + stop;
    
    if (start < 0) start = 0;
    if (stop >= list->length) stop = list->length - 1;
    
    if (start > stop || start >= list->length) {
        *count = 0;
        return NULL;
    }
    
    int result_count = stop - start + 1;
    char **result = malloc(sizeof(char*) * result_count);
    if (!result) {
        *count = 0;
        return NULL;
    }
    
    // Find starting node
    list_node_t *node = list->head;
    for (int i = 0; i < start; i++) {
        node = node->next;
    }
    
    for (int i = 0; i < result_count && node; i++) {
        result[i] = (char *)node->data;  
        node = node->next;
    }
    
    *count = result_count;
    return result;
}