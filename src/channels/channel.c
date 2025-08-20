#include "channel.h"

channel_t *create_channel(char *name) {
    channel_t *channel = malloc(sizeof(channel_t));
    if (!channel) {
        return NULL;
    }
    
    channel->name = strdup(name);
    if (!channel->name) {
        free(channel);
        return NULL;
    }
    
    channel->clients = list_create();
    if (!channel->clients) {
        free(channel->name);
        free(channel);
        return NULL;
    }
    
    channel->n_clients = 0;
    return channel;
}

void destroy_channel(channel_t *channel) {
    if (!channel) return ;
    
    if (channel->name) {
        free(channel->name);
    }
    
    if (channel->clients) {
        list_destroy(channel->clients);
    }
    
    free(channel);
}
