#ifndef CHANNEL_H
#define CHANNEL_H
#include "../lib/list.h"

typedef struct channel
{
  char *name;
  redis_list_t *clients;
  int n_clients;  
}channel_t;


channel_t *create_channel(char *name);
void destroy_channel(channel_t *name);


#endif