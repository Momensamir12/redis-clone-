#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include "../event_loop/event_loop.h"
#include "../server/server.h"  
#include "../redis_db/redis_db.h"
#include "../lib/list.h"


typedef enum {
    MASTER,
    SLAVE
} role_t;

typedef struct replication_info{
  role_t role;
  u_int16_t connected_slaves;
  int master_port;
  char *master_host;
  char replication_id[41];
  uint64_t master_repl_offset;
  u_int16_t master_fd;
  uint16_t handshake_step; 

} replication_info_t;
typedef struct redis_server {
    server_t *server;
    event_loop_t *event_loop;
    redis_db_t *db;
    redis_list_t *clients;
    redis_list_t *blocked_clients;
    replication_info_t *replication_info;
    

} redis_server_t;

redis_server_t* redis_server_create(int port);
void redis_server_destroy(redis_server_t *redis);
void redis_server_run(redis_server_t *redis);

int redis_server_configure_master(redis_server_t *server);
int redis_server_configure_replica(redis_server_t *server, char* master_host, int master_port);

#endif // REDIS_SERVER_H