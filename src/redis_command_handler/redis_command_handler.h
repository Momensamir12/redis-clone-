#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include "../redis_db/redis_db.h"
#include "../redis_server/redis_server.h"
// Command handler function type
typedef char* (*command_handler_t)(redis_server_t *server, char **args, int argc, void *client);

typedef struct redis_command {
    char *name;
    command_handler_t handler;
    int min_args;
    int max_args;
} redis_command_t;
// Initialize command table
void init_command_table(void);

// Main command handler
char *handle_command(redis_server_t *server, char *buffer, void *client);

// Command handlers
char *handle_echo_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_ping_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_set_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_get_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_rpush_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_lpush_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_llen_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_rpop_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_lpop_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_lrange_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_blpop_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_type_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_xadd_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_xrange_command(redis_server_t *server, char **args, int argc, void *client);
char *handle_xread_command(redis_server_t *server, char **args, int argc, void *client);

void check_blocked_clients_timeout(redis_server_t *server);




#endif
