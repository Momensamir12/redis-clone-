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

void client_block(client_t *client, const char *key, time_t timeout_timestamp)
{
    if (!client) return;
    
    printf("client_block: BEFORE - Client fd=%d, current block_timeout=%ld\n", 
           client->fd, client->block_timeout);
    
    client->is_blocked = true;
    
    // Clear any existing blocked key
    if (client->blocked_key) {
        free(client->blocked_key);
        client->blocked_key = NULL;
    }
    
    // Set new blocked key if provided
    if (key) {
        client->blocked_key = strdup(key);
    }
    
    // Set the timeout
    client->block_timeout = timeout_timestamp;
    
    printf("client_block: AFTER - Client fd=%d blocked until %ld (current: %ld)\n", 
           client->fd, client->block_timeout, time(NULL));
}

void client_unblock(client_t *client) {
    if (!client) return;
    
    client->is_blocked = 0;
    client->block_timeout = 0;
    
    if (client->blocked_key) {
        free(client->blocked_key);
        client->blocked_key = NULL;
    }
}

void client_unblock_stream(client_t *client)
{
    if (!client) return;
    
    // Clean up XREAD-specific data
    if (client->xread_streams) {
        for (int i = 0; i < client->xread_num_streams; i++) {
            free(client->xread_streams[i]);
        }
        free(client->xread_streams);
        client->xread_streams = NULL;
    }
    
    if (client->xread_start_ids) {
        for (int i = 0; i < client->xread_num_streams; i++) {
            free(client->xread_start_ids[i]);
        }
        free(client->xread_start_ids);
        client->xread_start_ids = NULL;
    }
    
    client->xread_num_streams = 0;
    client->stream_block = 0;
    
    client_unblock(client);
}
