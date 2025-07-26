#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include "../redis_db/redis_db.h"

// Command handler function type
typedef char *(*command_handler_fn)(redis_db_t *db, char **args, int argc);

// Command structure
typedef struct redis_command {
    char *name;
    command_handler_fn handler;
    int min_args;  // Minimum number of arguments (including command name)
    int max_args;  // Maximum number of arguments (-1 for unlimited)
} redis_command_t;

// Initialize command table
void init_command_table(void);

// Main command handler
char *handle_command(redis_db_t *db, char *buffer);

// Command handlers
char *handle_echo_command(redis_db_t *db, char **args, int argc);
char *handle_ping_command(redis_db_t *db, char **args, int argc);
char *handle_set_command(redis_db_t *db, char **args, int argc);
char *handle_get_command(redis_db_t *db, char **args, int argc);
char *handle_rpush_command(redis_db_t *db, char **args, int argc);
char *handle_lpush_command(redis_db_t *db, char **args, int argc);
char *handle_llen_command(redis_db_t *db, char **args, int argc);
char *handle_rpop_command(redis_db_t *db, char **args, int argc);
char *handle_lpop_command(redis_db_t *db, char **args, int argc);
char *handle_lrange_command(redis_db_t *db, char **args, int argc);


#endif
