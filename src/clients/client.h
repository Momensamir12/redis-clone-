#ifndef CLIENT_H
#define CLIENT_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <stdbool.h>
#include "../lib/list.h"

typedef struct client {
    int fd;
    int is_blocked;
    time_t block_timeout;
    long long block_timeout_ms;
    char *blocked_key;
    bool stream_block;
    char **xread_streams;      
    char **xread_start_ids;   
    int xread_num_streams; 
    int is_queued; /* is the client queueing commands using multi*/
    redis_list_t *transaction_commands;
    char **channels;
    int subscribed_channels;
    int sub_mode;
}client_t;

typedef struct transaction_command {
    char *buffer;       // Original command buffer
    char **args;        // Parsed arguments  
    int argc;          
} transaction_command_t;

client_t *create_client(int fd);
void add_client_to_list(redis_list_t *list, client_t *client);
void remove_client_from_list(redis_list_t *list, client_t *client);
void client_block(client_t *client, const char *key, int timeout);
void client_unblock_stream(client_t *client);
void client_unblock(client_t *client);
void cleanup_transaction(client_t *c);
void free_client(client_t *client);

#endif