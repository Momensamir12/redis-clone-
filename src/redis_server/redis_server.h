#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include "../event_loop/event_loop.h"
#include "../server/server.h"  
#include "../redis_db/redis_db.h"
typedef struct redis_server {
    server_t *server;
    event_loop_t *event_loop;
    redis_db_t *db;
} redis_server_t;

redis_server_t* redis_server_create(int port);
void redis_server_destroy(redis_server_t *redis);
void redis_server_run(redis_server_t *redis);

#endif // REDIS_SERVER_H