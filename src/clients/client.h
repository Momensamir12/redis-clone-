#ifndef CLIENT_H
#define CLIENT_h
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "../lib/list.h"

typedef struct client {
    int fd;
    int is_blocked;
    time_t block_timeout;
    char *blocked_key;
}client_t;

client_t *create_client(int fd);
void add_client_to_list(redis_list_t *list, client_t *client);
void remove_client_from_list(redis_list_t *list, client_t *client);
void client_block(client_t *client, const char *key, int timeout);
void client_unblock(client_t *client);
void free_client(client_t *client);

#endif