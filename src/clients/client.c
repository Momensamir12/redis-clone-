#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "../lib/list.h"
#include "client.h"
#include <time.h>

client_t *create_client(int fd)
{
    client_t *client = malloc(sizeof(client_t));
    if(!client)
      return NULL;
    client->fd = fd;
    client->block_timeout = 0;
    client->is_blocked = 0;
    client->blocked_key = NULL;
    
    return client;
}


void add_client_to_list(redis_list_t *list, client_t *client) {
    if (!list || !client) return;
    list_rpush(list, client);
}


void remove_client_from_list(redis_list_t *list, client_t *client) {
    if (!list || !client) return;
    list_remove(list, client);  
}

void free_client(client_t *client) {
    if (!client) return;
    
    if (client->blocked_key) {
        free(client->blocked_key);
    }
    free(client);
}

void client_block(client_t *client, const char *key, int timeout) {
    if (!client) return;
    
    client->is_blocked = 1;
    
    if (timeout > 0) {
        client->block_timeout = time(NULL) + timeout;
    } else {
        client->block_timeout = 0;
    }
    
    if (client->blocked_key) {
        free(client->blocked_key);
    }
    
    client->blocked_key = key ? strdup(key) : NULL;
}

void client_unblock(client_t *client) {
    if (!client) return;
    
    client->is_blocked = 0;
    client->block_timeout = 0;
    client->stream_block = 0;
    if (client->blocked_key) {
        free(client->blocked_key);
        client->blocked_key = NULL;
    }
}

