#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <sys/stat.h>
#include "../resp_praser/resp_parser.h"
#include "redis_command_handler.h"
#include "../redis_db/redis_db.h"
#include "../expiry_utils/expiry_utils.h"
#include "../lib/list.h"
#include "../redis_server/redis_server.h"
#include "../clients/client.h"
#include <math.h>
#include "../streams/redis_stream.h"
#include "../lib/radix_tree.h"
#include "../lib/utils.h"
#include "../rdb/rdb.h"
#include "../rdb/io_buffer.h"

#define NULL_RESP_VALUE "$-1\r\n"
#define PSYNC_RESPONSE_SIZE 1024
#define RDB_RESPONSE_SIZE 4096
#define RDB_DEFAULT_DIR  "/tmp/redis-files"
#define RDB_TEMP_FILE "temp.rdb"
#define RDB_DEFAULT_FILE "dump.rdb"
#define RESP_DEFAULT_ERROR "-ERR unknown error\r\n"
#define RESP_MEMORY_ERROR "-ERR out of memory\r\n"
// Global command hash table
static hash_table_t *command_table = NULL;

// Command definitions
static redis_command_t commands[] = {
    {"echo", handle_echo_command, 2, 2},
    {"ping", handle_ping_command, 1, 2},
    {"set", handle_set_command, 3, -1},
    {"get", handle_get_command, 2, 2},
    {"rpush", handle_rpush_command, 3, -1},
    {"lpush", handle_lpush_command, 3, -1},
    {"llen", handle_llen_command, 2, 2},
    {"rpop", handle_rpop_command, 2, 2},
    {"lpop", handle_lpop_command, 2, 3},
    {"lrange", handle_lrange_command, 4, 4},
    {"blpop", handle_blpop_command, 3, -1},
    {"type", handle_type_command, 2, 2},
    {"xadd", handle_xadd_command, 4, -1},
    {"xrange", handle_xrange_command, 4, 6},
    {"xread", handle_xread_command, 4, -1},
    {"incr", handle_incr_command,2 , -1},
    {"multi", handle_multi_command, 1, 1},
    {"exec", handle_exec_command, 1, 1},
    {"discard", handle_discard_command, 1, 1},
    {"info", handle_info_command, 2, -1},
    {"replconf", handle_replconf_command, 2, -1},
    {"psync", handle_psync_command, 3, -1},
    {"wait", handle_wait_command, 3, 3},
    {"config",handle_config_get_command, 2 , -1},
    {"keys", handle_keys_command, 2, 2},
    {"subscribe", handle_subscribe_command, 2, 2},

    {NULL, NULL, 0, 0} // Sentinel
};

static const char *pubsub_allowed_commands[] = {
    "subscribe",
    "unsubscribe", 
    "psubscribe",
    "punsubscribe",
    "ping",
    "quit",
    "reset",
    NULL  
};


void init_command_table(void)
{
    if (command_table != NULL)
        return; 

    command_table = hash_table_create(32); 

    for (int i = 0; commands[i].name != NULL; i++)
    {
        hash_table_set(command_table, commands[i].name, &commands[i]);

        char *lower = strdup(commands[i].name);
        for (char *p = lower; *p; p++)
        {
            *p = tolower(*p);
        }
        hash_table_set(command_table, lower, &commands[i]);
        free(lower);
    }
}

static int extract_timeout(char *timeout);
static char *build_xread_response_for_blocked_client(redis_server_t *server, client_t *client, const char *stream_key, const char *new_id);
static void add_command_to_transaction(redis_server_t *server, char *buffer, char **args, int argc, void *client);


static int create_rdb_snapshot(redis_db_t *db);
static int send_rdb_file_to_client(int client_fd, const char *rdb_path);
static long get_file_size_stat(const char *filepath);
static int rename_rdb_file(const char *temp_path, const char *main_path);
int add_replica(redis_server_t *server, int replica_fd);
static int is_pubsub_command(const char *command);
// Parse all arguments into an array
static char **parse_command_args(resp_buffer_t *resp_buffer, int *argc)
{
    char *array_count = parse_resp_array(resp_buffer);
    if (!array_count)
        return NULL;

    *argc = atoi(array_count);
    free(array_count);

    if (*argc <= 0)
        return NULL;

    char **args = calloc(*argc, sizeof(char *));
    if (!args)
        return NULL;

    for (int i = 0; i < *argc; i++)
    {
        args[i] = parse_resp_array(resp_buffer);
        if (!args[i])
        {
            for (int j = 0; j < i; j++)
            {
                free(args[j]);
            }
            free(args);
            return NULL;
        }
    }

    return args;
}

// Free argument array
static void free_command_args(char **args, int argc)
{
    if (!args)
        return;
    for (int i = 0; i < argc; i++)
    {
        if (args[i])
            free(args[i]);
    }
    free(args);
}

char *handle_command(redis_server_t *server, char *buffer, void *client)
{
    if (!server || !buffer)
        return NULL;

    client_t *c = (client_t *)client;
    
    resp_buffer_t *resp_buffer = calloc(1, sizeof(resp_buffer_t));
    if (!resp_buffer)
        return NULL;

    resp_buffer->buffer = buffer;
    resp_buffer->size = strlen(buffer);
    resp_buffer->pos = 0;

    int argc;
    char **args = parse_command_args(resp_buffer, &argc);
    if (!args || argc < 1)
    {
        free(resp_buffer);
        return strdup("-ERR protocol error\r\n");
    }

    char *cmd_lower = strdup(args[0]);
    for (char *p = cmd_lower; *p; p++)
    {
        *p = tolower(*p);
    }

    if (c && c->is_queued && 
        strcmp(cmd_lower, "exec") != 0 && 
        strcmp(cmd_lower, "discard") != 0 &&
        strcmp(cmd_lower, "multi") != 0)
    {
        // Queue the command instead of executing
        add_command_to_transaction(server, buffer, args, argc, client);
        free(cmd_lower);
        free_command_args(args, argc);
        free(resp_buffer);
        return strdup("+QUEUED\r\n");
    }

    // Look up and execute command normally...
    redis_command_t *cmd = (redis_command_t *)hash_table_get(command_table, cmd_lower);

    if (!cmd)
    {
        char response[256];
        sprintf(response, "-ERR unknown command '%s'\r\n", args[0]);
        free_command_args(args, argc);
        free(resp_buffer);
        return strdup(response);
    }

    // Validate argument count
    if (argc < cmd->min_args || (cmd->max_args != -1 && argc > cmd->max_args))
    {
        char response[256];
        sprintf(response, "-ERR wrong number of arguments for '%s' command\r\n", cmd->name);
        free_command_args(args, argc);
        free(resp_buffer);
        return strdup(response);
    }
       if (c && c->sub_mode) {
        if (!is_pubsub_command(cmd_lower)) {
            char response[256];
            snprintf(response, sizeof(response), 
                    "-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", 
                    args[0]);
            
            free_command_args(args, argc);
            free(resp_buffer);
            return strdup(response);
        }
    }
    
    char *response = cmd->handler(server, args, argc, client);
    free(cmd_lower);
    free_command_args(args, argc);
    free(resp_buffer);
    return response;
}
// For BLPOP - timeout is in seconds (can be fractional)
static long long extract_blpop_timeout_ms(char *timeout_str)
{
    double timeout_seconds = atof(timeout_str);

    printf("extract_blpop_timeout_ms: input='%s', parsed=%f seconds\n", timeout_str, timeout_seconds);

    if (timeout_seconds == 0.0)
    {
        return 0; // Wait forever
    }

    // Convert seconds to milliseconds
    long long timeout_ms = (long long)(timeout_seconds * 1000.0);

    printf("extract_blpop_timeout_ms: %f seconds -> %lld ms\n", timeout_seconds, timeout_ms);
    return timeout_ms;
}

// For XREAD - timeout is already in milliseconds
static long long extract_xread_timeout_ms(char *timeout_str)
{
    long long timeout_ms = atoll(timeout_str);
    printf("extract_xread_timeout_ms: input='%s' -> %lld ms\n", timeout_str, timeout_ms);
    return timeout_ms;
}
// Command handlers
static void check_blocked_clients_for_key(redis_server_t *server, const char *key, const char *notify)
{
    if (!server || !server->blocked_clients || !key)
        return;

    list_node_t *node = server->blocked_clients->head;
    while (node)
    {
        list_node_t *next = node->next;
        client_t *blocked_client = (client_t *)node->data;

        if (blocked_client->blocked_key &&
            strcmp(blocked_client->blocked_key, key) == 0)
        {

            // Check if key now has data
            redis_object_t *obj = hash_table_get(server->db->dict, key);
            if (obj && obj->type == REDIS_LIST)
            {
                redis_list_t *list = (redis_list_t *)obj->ptr;
                if (list_length(list) > 0)
                {
                    // Pop value
                    char *value = (char *)list_lpop(list);

                    if (value)
                    {
                        // Send response to unblocked client
                        char response[1024];
                        snprintf(response, sizeof(response),
                                 "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                                 strlen(key), key, strlen(value), value);
                        send(blocked_client->fd, response, strlen(response), MSG_NOSIGNAL);

                        // Unblock client
                        client_unblock(blocked_client);
                        remove_client_from_list(server->blocked_clients, blocked_client);

                        free(value);

                        printf("Unblocked client fd=%d with data from key '%s'\n",
                               blocked_client->fd, key);
                    }
                }
            }
            if (obj && obj->type == REDIS_STREAM)
            {
                send(blocked_client->fd, notify, strlen(notify), MSG_NOSIGNAL);
            }
        }
        node = next;
    }
}

static void check_blocked_clients_for_stream(redis_server_t *server, const char *key, const char *new_id)
{
    if (!server || !server->blocked_clients || !key || !new_id)
        return;

    printf("check_blocked_clients_for_stream: key='%s', new_id='%s', blocked_clients=%zu\n",
           key, new_id, list_length(server->blocked_clients));

    list_node_t *node = server->blocked_clients->head;
    while (node)
    {
        list_node_t *next = node->next;
        client_t *blocked_client = (client_t *)node->data;

        printf("Checking client fd=%d: stream_block=%d, has_xread_streams=%d\n",
               blocked_client->fd, blocked_client->stream_block,
               blocked_client->xread_streams != NULL);

        if (blocked_client->stream_block && blocked_client->xread_streams)
        {
            // Check if this stream is in the client's XREAD list
            for (int i = 0; i < blocked_client->xread_num_streams; i++)
            {
                printf("Comparing stream '%s' with client's stream[%d] '%s'\n",
                       key, i, blocked_client->xread_streams[i]);

                if (strcmp(blocked_client->xread_streams[i], key) == 0)
                {
                    printf("Found matching stream! Building response...\n");

                    // Build XREAD response for this client
                    char *response = build_xread_response_for_blocked_client(server, blocked_client, key, new_id);
                    if (response)
                    {
                        printf("Sending response to client fd=%d: %.100s...\n",
                               blocked_client->fd, response);

                        send(blocked_client->fd, response, strlen(response), MSG_NOSIGNAL);

                        // Unblock client
                        client_unblock_stream(blocked_client);
                        remove_client_from_list(server->blocked_clients, blocked_client);

                        free(response);
                        printf("Unblocked XREAD client fd=%d with new entry from stream '%s'\n",
                               blocked_client->fd, key);
                    }
                    else
                    {
                        printf("Failed to build response for client fd=%d\n", blocked_client->fd);
                    }
                    break;
                }
            }
        }
        node = next;
    }
}

// Update command handlers to use server context
char *handle_echo_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)server;
    (void)client;
    (void)argc;
    return encode_bulk_string(args[1]);
}

char *handle_ping_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)server;
    client_t *c = (client_t *)client;
    
    if (c->sub_mode) {
        // In pub/sub mode, PING returns: ["pong", ""] (2 elements)
        char **response_args = malloc(2 * sizeof(char *));  // Fixed: don't shadow args
        if (!response_args) {
            return strdup("-ERR out of memory\r\n");
        }
        
        response_args[0] = strdup("pong");
        response_args[1] = strdup("");  // Empty string
        
        char *result = encode_resp_array(response_args, 2);
        
        // Clean up
        free(response_args[0]);
        free(response_args[1]);
        free(response_args);
        
        return result;
    }
    
    // Normal mode (not pub/sub)
    if (argc == 2) {
        return encode_bulk_string(args[1]);  // Echo the message
    }
    
    return encode_simple_string("PONG");
}

char *handle_set_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    char *key = args[1];
    char *value = args[2];
    char *expiry_ms = NULL;

    // Parse optional arguments
    int i = 3;
    while (i < argc)
    {
        if (strcasecmp(args[i], "px") == 0 && i + 1 < argc)
        {
            expiry_ms = args[i + 1];
            i += 2;
        }
        else if (strcasecmp(args[i], "ex") == 0 && i + 1 < argc)
        {
            int seconds = atoi(args[i + 1]);
            char ms_buf[32];
            sprintf(ms_buf, "%d", seconds * 1000);
            expiry_ms = ms_buf;
            i += 2;
        }
        else
        {
            return strdup("-ERR syntax error\r\n");
        }
    }
    redis_object_t *obj;
    if(isInteger(value))
      obj = redis_object_create_number(value);

    else  
      obj = redis_object_create_string(value);

    if (expiry_ms)
    {
        set_expiry_ms(obj, atoi(expiry_ms));
    }

    hash_table_set(server->db->dict, strdup(key), obj);
    return encode_simple_string("OK");
}

char *handle_get_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)argc;
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj)
    {
        return strdup(NULL_RESP_VALUE);
    }

    if (is_expired(obj))
    {
        redis_object_destroy(obj);
        hash_table_delete(server->db->dict, key);
        return strdup(NULL_RESP_VALUE);
    }

    if (obj->type != REDIS_STRING && obj->type != REDIS_NUMBER)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    return encode_bulk_string((char *)obj->ptr);
}

// LPUSH with blocking client notification
// In redis_command_handler.c - Fix RPUSH
char *handle_rpush_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj)
    {
        obj = redis_object_create_list();
        hash_table_set(server->db->dict, strdup(key), obj);
    }
    else if (obj->type != REDIS_LIST)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;

    // Push all values
    for (int i = 2; i < argc; i++)
    {
        list_rpush(list, strdup(args[i]));
    }

    size_t list_len = list_length(list);

    check_blocked_clients_for_key(server, key, NULL);

    char response[32];
    sprintf(response, ":%zu\r\n", list_len);
    return strdup(response);
}

// Same fix for LPUSH
char *handle_lpush_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj)
    {
        obj = redis_object_create_list();
        hash_table_set(server->db->dict, strdup(key), obj);
    }
    else if (obj->type != REDIS_LIST)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;

    // Push all values
    for (int i = 2; i < argc; i++)
    {
        list_lpush(list, strdup(args[i]));
    }

    // Get the length BEFORE checking blocked clients
    size_t list_len = list_length(list);

    // Check if any clients are blocked on this key
    check_blocked_clients_for_key(server, key, NULL);

    // Return the original length
    char response[32];
    sprintf(response, ":%zu\r\n", list_len);
    return strdup(response);
}

// BLPOP implementation
// In handle_blpop_command - Fix timeout parsing
// In handle_blpop_command - Simple fix without ceil()
char *handle_blpop_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (!client || !server)
    {
        return strdup("-ERR internal error\r\n");
    }

    client_t *c = (client_t *)client;

    // Parse timeout as milliseconds
    long long timeout_ms = extract_blpop_timeout_ms(args[argc - 1]);

    // Check if client is already blocked
    if (c->is_blocked)
    {
        return strdup("-ERR client already blocked\r\n");
    }

    // Try each key (except last arg which is timeout)
    for (int i = 1; i < argc - 1; i++)
    {
        char *key = args[i];
        redis_object_t *obj = hash_table_get(server->db->dict, key);

        if (obj && obj->type == REDIS_LIST)
        {
            redis_list_t *list = (redis_list_t *)obj->ptr;
            if (list_length(list) > 0)
            {
                // Can pop immediately
                char *value = (char *)list_lpop(list);
                if (value)
                {
                    char response[1024];
                    snprintf(response, sizeof(response),
                             "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                             strlen(key), key, strlen(value), value);
                    free(value);
                    return strdup(response);
                }
            }
        }
    }

    // No keys have values - block the client with millisecond precision
    long long current_time_ms = get_current_time_ms();
    long long timeout_timestamp_ms = 0;

    if (timeout_ms > 0)
    {
        timeout_timestamp_ms = current_time_ms + timeout_ms;
        printf("Setting BLPOP timeout for client fd=%d: current=%lld + %lld = %lld ms\n",
               c->fd, current_time_ms, timeout_ms, timeout_timestamp_ms);
    }

    // Use millisecond timeout for BLPOP
    c->block_timeout_ms = timeout_timestamp_ms;
    c->is_blocked = true;
    c->blocked_key = strdup(args[1]); // Block on first key
    add_client_to_list(server->blocked_clients, c);

    printf("Client fd=%d blocked on key '%s' with timeout %lld ms (until %lld)\n",
           c->fd, args[1], timeout_ms, timeout_timestamp_ms);

    return NULL; // No response - client is blocked
}

// Other command handlers remain similar, just update signature...
char *handle_llen_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)argc;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj)
    {
        return strdup(":0\r\n");
    }

    if (obj->type != REDIS_LIST)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;
    char response[32];
    sprintf(response, ":%zu\r\n", list_length(list));
    return strdup(response);
}

char *handle_rpop_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)argc;
    (void)client;
    char *key = args[1];

    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj || obj->type != REDIS_LIST)
    {
        return strdup(NULL_RESP_VALUE);
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;
    char *value = (char *)list_rpop(list);

    if (!value)
    {
        return strdup(NULL_RESP_VALUE);
    }

    char *response = encode_bulk_string(value);
    free(value);
    return response;
}

char *handle_lpop_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    char *key = args[1];
    size_t count = 0;
    if (argc >= 3)
    {
        count = atoi(args[2]);
    }

    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj || obj->type != REDIS_LIST)
    {
        return strdup(NULL_RESP_VALUE);
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;

    if (count == 0)
        count = 1;

    char **values = calloc(count, sizeof(char *));
    int actual_count = 0;

    for (int i = 0; i < count; i++)
    {
        values[i] = (char *)list_lpop(list);
        if (values[i])
        {
            actual_count++;
        }
        else
        {
            break;
        }
    }

    if (actual_count == 0)
    {
        free(values);
        return strdup("*0\r\n");
    }

    char *response;
    if (argc < 3 && actual_count == 1)
    {
        // Single value response for LPOP without count
        response = encode_bulk_string(values[0]);
        free(values[0]);
    }
    else
    {
        // Array response for LPOP with count
        response = encode_resp_array(values, actual_count);
        for (int i = 0; i < actual_count; i++)
        {
            free(values[i]);
        }
    }

    free(values);
    return response;
}

char *handle_lrange_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    if (argc != 4)
    {
        return strdup("-ERR wrong number of arguments for 'lrange' command\r\n");
    }

    char *key = args[1];
    int start = atoi(args[2]);
    int stop = atoi(args[3]);

    redis_object_t *obj = hash_table_get(server->db->dict, key);
    if (!obj)
    {
        return strdup("*0\r\n");
    }

    if (obj->type != REDIS_LIST)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;
    int count;
    char **values = list_range(list, start, stop, &count);

    if (!values)
    {
        return strdup("*0\r\n");
    }

    char *response = encode_resp_array(values, count);
    free(values);

    return response;
}

void check_blocked_clients_timeout(redis_server_t *server)
{
    if (!server || !server->blocked_clients)
        return;

    long long now_ms = get_current_time_ms();
    list_node_t *node = server->blocked_clients->head;

    printf("Checking timeouts: current time=%lld ms, blocked clients=%zu\n",
           now_ms, list_length(server->blocked_clients));

    while (node)
    {
        list_node_t *next = node->next;
        client_t *client = (client_t *)node->data;

        // Both XREAD and BLPOP now use millisecond precision
        printf("Client fd=%d: stream_block=%d, block_timeout_ms=%lld, current=%lld\n",
               client->fd, client->stream_block, client->block_timeout_ms, now_ms);

        if (client->block_timeout_ms > 0 && client->block_timeout_ms <= now_ms)
        {
            printf("Client fd=%d timed out! timeout=%lld, now=%lld\n",
                   client->fd, client->block_timeout_ms, now_ms);

            const char *nil_response = "*-1\r\n";
            send(client->fd, nil_response, strlen(nil_response), MSG_NOSIGNAL);

            if (client->stream_block)
            {
                client_unblock_stream(client);
                printf("Sent timeout response to XREAD client fd=%d\n", client->fd);
            }
            else
            {
                client_unblock(client);
                printf("Sent timeout response to BLPOP client fd=%d\n", client->fd);
            }

            remove_client_from_list(server->blocked_clients, client);
        }
        else if (client->blocked_key && !client->stream_block)
        {
            // Handle list blocking - check if data is available
            redis_object_t *obj = hash_table_get(server->db->dict, client->blocked_key);
            if (obj && obj->type == REDIS_LIST)
            {
                redis_list_t *list = (redis_list_t *)obj->ptr;
                if (list_length(list) > 0)
                {
                    char *value = (char *)list_lpop(list);
                    if (value)
                    {
                        char response[1024];
                        snprintf(response, sizeof(response),
                                 "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                                 strlen(client->blocked_key), client->blocked_key,
                                 strlen(value), value);
                        send(client->fd, response, strlen(response), MSG_NOSIGNAL);

                        client_unblock(client);
                        remove_client_from_list(server->blocked_clients, client);

                        free(value);
                        printf("Client fd=%d unblocked by timer check\n", client->fd);
                    }
                }
            }
        }

        node = next;
    }
}
char *handle_type_command(redis_server_t *server, char **args, int argc, void *client)
{
    char *key = args[1];
    void *value = hash_table_get(server->db->dict, key);
    if (!value)
    {
        return encode_simple_string("none");
    }
    redis_object_t *obj = (redis_object_t *)value;
    char *response = encode_simple_string(redis_type_to_string(obj->type));

    free(value);
    return response;
}

char *handle_xadd_command(redis_server_t *server, char **args, int argc, void *client)
{
    client_t *c = (client_t *)(client);

    // XADD key ID field value [field value ...]
    if (argc < 5 || (argc - 3) % 2 != 0)
    {
        return strdup("-ERR wrong number of arguments for 'xadd' command\r\n");
    }

    char *key = args[1];
    char *id = args[2];

    size_t field_count = (argc - 3) / 2;

    // Extract field names and values
    const char **field_names = malloc(field_count * sizeof(char *));
    const char **field_values = malloc(field_count * sizeof(char *));

    if (!field_names || !field_values)
    {
        free(field_names);
        free(field_values);
        return strdup("-ERR out of memory\r\n");
    }

    for (size_t i = 0; i < field_count; i++)
    {
        field_names[i] = args[3 + i * 2];
        field_values[i] = args[3 + i * 2 + 1];
    }

    // Get or create stream
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    redis_stream_t *stream = NULL;

    if (!obj)
    {
        // Create new stream
        stream = redis_stream_create();
        if (!stream)
        {
            free(field_names);
            free(field_values);
            return strdup("-ERR failed to create stream\r\n");
        }

        // Create redis object wrapper
        obj = redis_object_create_stream(stream);
        if (!obj)
        {
            redis_stream_destroy(stream);
            free(field_names);
            free(field_values);
            return strdup("-ERR failed to create stream object\r\n");
        }

        // Store in database
        hash_table_set(server->db->dict, strdup(key), obj);
    }
    else
    {
        // Check if it's a stream
        if (obj->type != REDIS_STREAM)
        {
            free(field_names);
            free(field_values);
            return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        }
        stream = (redis_stream_t *)obj->ptr;
    }

    // Add entry to stream
    int error_code = 0;
    char *generated_id = redis_stream_add(stream, id, field_names, field_values, field_count, &error_code);

    free(field_names);
    free(field_values);

    if (!generated_id)
    {
        // Return specific error message based on error code
        switch (error_code)
        {
        case 1:
            return strdup("-ERR Invalid stream ID specified as stream command argument\r\n");
        case 2:
            return strdup("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
        case 3:
            return strdup("-ERR out of memory\r\n");
        case 4:
            return strdup("-ERR invalid parameters\r\n");
        case 5:
            return strdup("-ERR failed to create stream entry\r\n");
        case 6: // Special case for 0-0
            return strdup("-ERR The ID specified in XADD must be greater than 0-0\r\n");
        default:
            return strdup(RESP_DEFAULT_ERROR);
        }
    }

    // Return the generated ID
    char *response = encode_bulk_string(generated_id);

    /*check if any clients are blocked on this stream , if so send the new entry*/
    check_blocked_clients_for_stream(server, key, generated_id);

    free(generated_id);

    return response;
}
static int extract_timeout(char *timeout_st)
{
    double timeout_float = atof(timeout_st);
    int timeout_seconds;

    printf("extract_timeout: input='%s', parsed=%f milliseconds\n", timeout_st, timeout_float);

    // Convert milliseconds to seconds
    double timeout_sec_float = timeout_float / 1000.0;

    // Handle fractional seconds
    if (timeout_sec_float == 0.0)
    {
        timeout_seconds = 0; // Wait forever
    }
    else if (timeout_sec_float > 0.0 && timeout_sec_float < 1.0)
    {
        timeout_seconds = 1; // Minimum 1 second for fractional values
    }
    else
    {
        timeout_seconds = (int)timeout_sec_float; // Truncate to seconds
        // Add 1 if there's a fractional part
        if (timeout_sec_float > timeout_seconds)
        {
            timeout_seconds++;
        }
    }

    printf("extract_timeout: %f ms -> %d seconds\n", timeout_float, timeout_seconds);
    return timeout_seconds;
}

char *handle_xrange_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;

    if (argc < 4)
    {
        return strdup("-ERR wrong number of arguments for 'xrange' command\r\n");
    }

    char *key = args[1];
    char *start_id = args[2];
    char *end_id = args[3];

    // Get stream from database
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    if (!obj)
    {
        return strdup("*0\r\n");
    }

    if (obj->type != REDIS_STREAM)
    {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }

    redis_stream_t *stream = (redis_stream_t *)obj->ptr;

    // Get range results from radix tree
    void **raw_results = NULL;
    int result_count = 0;

    radix_tree_range(stream->entries_tree, start_id, end_id, &raw_results, &result_count);

    if (!raw_results || result_count == 0)
    {
        free(raw_results);
        return strdup("*0\r\n");
    }

    // Build RESP response manually for the expected format
    size_t response_size = 1024;
    char *response = malloc(response_size);
    int pos = 0;

    pos += sprintf(response + pos, "*%d\r\n", result_count);

    for (int i = 0; i < result_count; i++)
    {
        stream_entry_t *entry = (stream_entry_t *)raw_results[i];

        // Each entry is [ID, [field1, value1, field2, value2, ...]]
        pos += sprintf(response + pos, "*2\r\n");

        // Add ID as bulk string
        pos += sprintf(response + pos, "$%zu\r\n%s\r\n", strlen(entry->id), entry->id);

        // Add fields as array
        pos += sprintf(response + pos, "*%zu\r\n", entry->field_count * 2);

        for (size_t j = 0; j < entry->field_count; j++)
        {
            // Field name
            pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                           strlen(entry->fields[j].name), entry->fields[j].name);
            // Field value
            pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                           strlen(entry->fields[j].value), entry->fields[j].value);
        }

        // Reallocate if needed
        if (pos > response_size - 1000)
        {
            response_size *= 2;
            response = realloc(response, response_size);
        }
    }

    free(raw_results);

    return response;
}

char *handle_xread_command(redis_server_t *server, char **args, int argc, void *client)
{
    client_t *c = (client_t *)(client);
    if (!client)
        return NULL;

    if (argc < 4)
    {
        return strdup("-ERR wrong number of arguments for 'xread' command\r\n");
    }

    // Check if client is already blocked
    if (c->is_blocked && (c->stream_block || c->blocked_key))
    {
        printf("Client fd=%d is already blocked\n", c->fd);
        return strdup("-ERR client already blocked\r\n");
    }

    // In handle_xread_command, replace the timeout parsing:
    long long timeout_ms = -1;
    bool is_blocking = false;

    // Parse BLOCK parameter
    for (int i = 0; i < argc; i++)
    {
        if (strcasecmp(args[i], "block") == 0 && i + 1 < argc)
        {
            timeout_ms = extract_xread_timeout_ms(args[i + 1]);
            if (timeout_ms < 0)
                return strdup("-ERR invalid block command arguments\r\n");
            is_blocking = true;
            break;
        }
    }
    // ... existing code for finding STREAMS and parsing arguments ...

    // Find STREAMS keyword
    int streams_pos = -1;
    for (int i = 1; i < argc; i++)
    {
        if (strcasecmp(args[i], "streams") == 0)
        {
            streams_pos = i;
            break;
        }
    }

    if (streams_pos == -1)
    {
        return strdup("-ERR syntax error\r\n");
    }

    // Calculate number of streams
    int remaining_args = argc - streams_pos - 1;
    if (remaining_args % 2 != 0)
    {
        return strdup("-ERR Unbalanced XREAD list of streams\r\n");
    }

    int num_streams = remaining_args / 2;
    if (num_streams == 0)
    {
        return strdup("-ERR wrong number of arguments for 'xread' command\r\n");
    }

    // Extract stream names and start IDs
    char **stream_keys = &args[streams_pos + 1];
    char **start_ids = &args[streams_pos + 1 + num_streams];

    // Build response - first count streams with data
    int streams_with_data = 0;
    void ***all_results = malloc(num_streams * sizeof(void **));
    int *all_counts = malloc(num_streams * sizeof(int));

    // Get results for each stream
    for (int i = 0; i < num_streams; i++)
    {
        all_results[i] = NULL;
        all_counts[i] = 0;

        redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, stream_keys[i]);
        if (!obj || obj->type != REDIS_STREAM)
        {
            continue;
        }

        redis_stream_t *stream = (redis_stream_t *)obj->ptr;

        // Get entries AFTER start_id (exclusive)
        char next_id[64];
        if (get_next_stream_id(start_ids[i], next_id, sizeof(next_id)) == 0)
        {
            radix_tree_range(stream->entries_tree, next_id, "999999999999999-999999999999999",
                             &all_results[i], &all_counts[i]);

            if (all_counts[i] > 0)
            {
                streams_with_data++;
            }
        }
    }

    printf("XREAD: is_blocking=%d, streams_with_data=%d\n", is_blocking, streams_with_data);

    // If blocking and no data, block the client
    if (is_blocking && streams_with_data == 0)
    {
        long long current_time_ms = get_current_time_ms();
        long long timeout_timestamp_ms = 0;

        if (timeout_ms > 0)
        {
            timeout_timestamp_ms = current_time_ms + timeout_ms;
            printf("Setting timeout for client fd=%d: current=%lld + %lld = %lld ms\n",
                   c->fd, current_time_ms, timeout_ms, timeout_timestamp_ms);
        }

        printf("Blocking client fd=%d on XREAD for %lld ms (until %lld)\n",
               c->fd, timeout_ms, timeout_timestamp_ms);

        // Store XREAD command info in client for later processing
        c->xread_streams = malloc(num_streams * sizeof(char *));
        c->xread_start_ids = malloc(num_streams * sizeof(char *));
        c->xread_num_streams = num_streams;

        for (int i = 0; i < num_streams; i++)
        {
            c->xread_streams[i] = strdup(stream_keys[i]);
            c->xread_start_ids[i] = strdup(start_ids[i]);
        }

        // Use millisecond timeout for XREAD
        c->block_timeout_ms = timeout_timestamp_ms;
        c->stream_block = true;
        c->is_blocked = true;
        add_client_to_list(server->blocked_clients, c);

        // Cleanup
        for (int i = 0; i < num_streams; i++)
        {
            free(all_results[i]);
        }
        free(all_results);
        free(all_counts);

        return NULL; // No response - client is blocked
    }

    if (streams_with_data == 0)
    {
        // Cleanup and return empty
        for (int i = 0; i < num_streams; i++)
        {
            free(all_results[i]);
        }
        free(all_results);
        free(all_counts);
        return strdup("*0\r\n");
    }

    // Build RESP response manually (same as before)
    size_t response_size = 4096;
    char *response = malloc(response_size);
    int pos = 0;

    pos += sprintf(response + pos, "*%d\r\n", streams_with_data);

    for (int i = 0; i < num_streams; i++)
    {
        if (all_counts[i] == 0)
            continue;

        // Stream entry: [stream_name, [entries]]
        pos += sprintf(response + pos, "*2\r\n");
        pos += sprintf(response + pos, "$%zu\r\n%s\r\n", strlen(stream_keys[i]), stream_keys[i]);
        pos += sprintf(response + pos, "*%d\r\n", all_counts[i]);

        for (int j = 0; j < all_counts[i]; j++)
        {
            stream_entry_t *entry = (stream_entry_t *)all_results[i][j];
            pos += sprintf(response + pos, "*2\r\n");
            pos += sprintf(response + pos, "$%zu\r\n%s\r\n", strlen(entry->id), entry->id);
            pos += sprintf(response + pos, "*%zu\r\n", entry->field_count * 2);

            for (size_t k = 0; k < entry->field_count; k++)
            {
                pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                               strlen(entry->fields[k].name), entry->fields[k].name);
                pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                               strlen(entry->fields[k].value), entry->fields[k].value);
            }
        }

        if (pos > response_size - 1000)
        {
            response_size *= 2;
            response = realloc(response, response_size);
        }
    }

    // Cleanup
    for (int i = 0; i < num_streams; i++)
    {
        free(all_results[i]);
    }
    free(all_results);
    free(all_counts);

    return response;
}

static char *build_xread_response_for_blocked_client(redis_server_t *server, client_t *client, const char *stream_key, const char *new_id)
{
    if (!server || !client || !stream_key || !new_id)
    {
        return NULL;
    }

    // Get the stream
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, stream_key);
    if (!obj || obj->type != REDIS_STREAM)
    {
        return NULL;
    }

    redis_stream_t *stream = (redis_stream_t *)obj->ptr;

    // Get the specific entry that was just added
    stream_entry_t *entry = (stream_entry_t *)radix_search(stream->entries_tree, (char *)new_id, strlen(new_id));
    if (!entry)
    {
        return NULL;
    }

    // Build RESP response: [[stream_name, [[entry_id, [field1, value1, ...]]]]]
    size_t response_size = 1024;
    char *response = malloc(response_size);
    int pos = 0;

    // Outer array with 1 stream
    pos += sprintf(response + pos, "*1\r\n");

    // Stream entry: [stream_name, [entries]]
    pos += sprintf(response + pos, "*2\r\n");

    // Add stream name
    pos += sprintf(response + pos, "$%zu\r\n%s\r\n", strlen(stream_key), stream_key);

    // Add entries array (1 entry)
    pos += sprintf(response + pos, "*1\r\n");

    // The entry: [ID, [field1, value1, field2, value2, ...]]
    pos += sprintf(response + pos, "*2\r\n");

    // Add ID
    pos += sprintf(response + pos, "$%zu\r\n%s\r\n", strlen(entry->id), entry->id);

    // Add fields array
    pos += sprintf(response + pos, "*%zu\r\n", entry->field_count * 2);

    for (size_t i = 0; i < entry->field_count; i++)
    {
        // Field name
        pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                       strlen(entry->fields[i].name), entry->fields[i].name);
        // Field value
        pos += sprintf(response + pos, "$%zu\r\n%s\r\n",
                       strlen(entry->fields[i].value), entry->fields[i].value);
    }

    return response;
}
char *handle_incr_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)argc;
    (void)client;
    
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj) {
        // Key doesn't exist - create as number starting at 1
        obj = redis_object_create_number("1");
        hash_table_set(server->db->dict, strdup(key), obj);
        return encode_number(strdup("1"));
    }
    
    if (obj->type != REDIS_NUMBER) {
        return strdup("-ERR value is not an integer or out of range\r\n");
    }
    
    char *value = (char *)obj->ptr;
    long long num = atoll(value);
    
    num++;
    
    char new_value[32];
    snprintf(new_value, sizeof(new_value), "%lld", num);
    
    free(obj->ptr);  // Free old string
    obj->ptr = strdup(new_value);  // Store new string
    
    return encode_number(strdup(new_value));
}

char *handle_multi_command(redis_server_t *server, char **args, int argc, void *client)
{
    client_t *c = (client_t *) client;
    if(!c)
      return NULL;

    c->transaction_commands = list_create();
    if(!c->transaction_commands)
      return NULL;
    c->is_queued = 1;
    
    return encode_simple_string("OK");
}

static void add_command_to_transaction(redis_server_t *server, char *buffer, char **args, int argc, void *client)
{
    (void)server;
    (void)args;  // Don't need parsed args
    (void)argc;  // Don't need arg count
    
    client_t *c = (client_t *)client;
    if (!c || !c->transaction_commands)
        return;
    
    // Just store the raw command buffer - much simpler!
    list_rpush(c->transaction_commands, strdup(buffer));
}

char *handle_exec_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)args;
    (void)argc;
    
    client_t *c = (client_t *)client;
    if (!c || !c->is_queued || !c->transaction_commands) {
        return strdup("-ERR EXEC without MULTI\r\n");
    }
    
    size_t command_count = list_length(c->transaction_commands);
    
    if (command_count == 0) {
        cleanup_transaction(c);
        return strdup("*0\r\n");
    }
    
    char **responses = calloc(command_count, sizeof(char *));
    if (!responses) {
        cleanup_transaction(c);
        return strdup("-ERR out of memory\r\n");
    }
    
    // Temporarily disable transaction mode
    c->is_queued = 0;
    
    // Execute each stored command buffer
    list_node_t *node = c->transaction_commands->head;
    for (size_t i = 0; i < command_count && node; i++) {
        char *command_buffer = (char *)node->data;
        
        // Let handle_command do all the parsing and execution
        responses[i] = handle_command(server, command_buffer, client);
        
        if (!responses[i]) {
            responses[i] = strdup("+OK\r\n");
        }
        
        node = node->next;
    }
    
    // Build RESP array response
    size_t total_size = 64;
    for (size_t i = 0; i < command_count; i++) {
        total_size += strlen(responses[i]);
    }
    
    char *final_response = malloc(total_size);
    int pos = sprintf(final_response, "*%zu\r\n", command_count);
    
    for (size_t i = 0; i < command_count; i++) {
        strcpy(final_response + pos, responses[i]);
        pos += strlen(responses[i]);
        free(responses[i]);
    }
    
    free(responses);
    cleanup_transaction(c);
    
    return final_response;
}

char *handle_discard_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)server;
    (void)args;
    (void)argc;
    
    client_t *c = (client_t *)client;
    if (!c || !c->is_queued)
    {
        return strdup("-ERR DISCARD without MULTI\r\n");
    }
    
    cleanup_transaction(c);
    return strdup("+OK\r\n");
}

char *handle_info_command(redis_server_t *server, char **args, int argc, void *client)
{
    (void)client;
    (void)args;
    (void)argc;
    
    if (!server->replication_info) {
        return strdup("-ERR server not configured\r\n");
    }
    
    char info_buffer[256];
    int offset = 0;
    if (server->replication_info->role == MASTER) {
        offset += snprintf(info_buffer + offset, sizeof(info_buffer) - offset,
            "role:master\r\nconnected_slaves:%d",
            server->replication_info->connected_slaves);
    } else if (server->replication_info->role == SLAVE) {
        offset += snprintf(info_buffer + offset, sizeof(info_buffer) - offset,
            "role:slave\r\nmaster_host:%s\r\nmaster_port:%d",
            server->replication_info->master_host ? server->replication_info->master_host : "unknown",
            server->replication_info->master_port);
    } else {
        return strdup("-ERR unknown role\r\n");
    }
    if(server->replication_info->replication_id && server->replication_info->master_repl_offset >=0)
    {
      offset += snprintf(info_buffer + offset, sizeof(info_buffer) - offset, "\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",server->replication_info->replication_id, 
        server->replication_info->master_repl_offset);
    }
   
    return encode_bulk_string(info_buffer);
}

char *handle_replconf_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (argc < 3) {
        return strdup("-ERR wrong number of arguments for 'replconf' command\r\n");
    }
    
    if (strcasecmp(args[1], "getack") == 0) {
        if (server->replication_info && server->replication_info->role == SLAVE) {
            char offset_str[32];
            snprintf(offset_str, sizeof(offset_str), "%lu", server->replication_info->replica_offset);
            int offset_digits = snprintf(NULL, 0, "%lu", server->replication_info->replica_offset);
            
            char response[256];
            snprintf(response, sizeof(response), 
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n",
                    offset_digits, offset_str);
            
            printf("Sending ACK with offset: %lu\n", server->replication_info->replica_offset);
            return strdup(response);
        } else {
            return strdup("-ERR GETACK can only be sent to replicas\r\n");
        }
    }
    
    // Handle REPLCONF ACK (sent by replica to master)
    else if (strcasecmp(args[1], "ack") == 0) {
        if (server->replication_info && server->replication_info->role == MASTER) {
            if (argc < 3) {
                return strdup("-ERR wrong number of arguments for 'replconf ack' command\r\n");
            }
            
            client_t *c = (client_t *)client;
            uint64_t ack_offset = strtoull(args[2], NULL, 10);
            
            // Find which replica this is and update its ACK offset
            for (int i = 0; i < MAX_REPLICAS; i++) {
                if (server->replication_info->replicas_fd[i] == c->fd) {
                    server->replication_info->replica_ack_offsets[i] = ack_offset;
                    printf("Updated replica %d (fd=%d) ACK offset to %lu\n", 
                           i, c->fd, ack_offset);
                    check_wait_completion(server);       
                    break;
                }
                
            }
            
            return NULL; // No response needed
        } else {
            return strdup("-ERR ACK can only be sent to masters\r\n");
        }
    }
    
    // Handle REPLCONF listening-port (sent by replica to master during handshake)
    else if (strcasecmp(args[1], "listening-port") == 0) {
        if (server->replication_info && server->replication_info->role == MASTER) {
            client_t *c = (client_t *)client;
            add_replica(server, c->fd);
            return encode_simple_string("OK");
        }
        return NULL;
    }
    
    // Handle REPLCONF capa (sent by replica to master during handshake)
    else if (strcasecmp(args[1], "capa") == 0) {
        if (server->replication_info && server->replication_info->role == MASTER) {
            // For now, just acknowledge the capability
            printf("Received replica capability: %s\n", argc > 2 ? args[2] : "unknown");
            return encode_simple_string("OK");
        }
        return strdup("-ERR CAPA can only be sent to masters\r\n");
    }
    
    return strdup("-ERR unknown REPLCONF option\r\n");
}

char *handle_psync_command(redis_server_t *server, char **args, int argc, void *client)
{
    char buffer[PSYNC_RESPONSE_SIZE];
    int offset = 0;
    
    if (strcmp(args[1], "?") == 0) {
        // Send FULLRESYNC response first
        offset += snprintf(buffer + offset, sizeof(buffer) - offset, 
                          "FULLRESYNC %s %d", 
                          server->replication_info->replication_id,
                          server->replication_info->master_repl_offset);
        
        client_t *client_conn = (client_t *)client;
        int client_fd = client_conn->fd; 
        
        int rdb_fd = create_rdb_snapshot(server->db); 
        if (rdb_fd == -1) {
            return encode_simple_string("ERR Failed to create RDB snapshot");
        }
        close(rdb_fd); 
        
        char *response = encode_simple_string(buffer);
        
        write(client_fd, response, strlen(response));
        free(response);
        
        if (send_rdb_file_to_client(client_fd, RDB_TEMP_FILE) == -1) {
            unlink(RDB_TEMP_FILE);
            return NULL; 
        }
        
        if (rename_rdb_file(RDB_TEMP_FILE, RDB_DEFAULT_FILE) == 0) {
            printf("RDB snapshot successfully saved as %s\n", RDB_DEFAULT_FILE);
        } else {
            fprintf(stderr, "Warning: Failed to rename RDB file to main file, cleaning up temp file\n");
            unlink(RDB_TEMP_FILE);
        }
        
        return NULL; 
    }
    
    return encode_simple_string("ERR Invalid PSYNC arguments");
}
char *handle_wait_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (argc != 3) {
        return strdup("-ERR wrong number of arguments for 'wait' command\r\n");
    }
    
    if (server->replication_info->role != MASTER) {
        return strdup("-ERR WAIT can only be used on masters\r\n");
    }
    
    int expected_replicas = atoi(args[1]);
    int timeout_ms = atoi(args[2]);
    
    printf("WAIT command: expecting %d replicas, timeout %d ms\n", expected_replicas, timeout_ms);
    
    // If no replicas connected, return 0 immediately
    if (server->replication_info->connected_slaves == 0) {
        return strdup(":0\r\n");
    }
    
    uint64_t current_offset = server->replication_info->master_repl_offset;
    
    // If offset is 0, all replicas are already synced
    if (current_offset == 0) {
        int synced_replicas = server->replication_info->connected_slaves;
        char response[32];
        sprintf(response, ":%d\r\n", synced_replicas);
        return strdup(response);
    }
    
    // Check if enough replicas are already synced
    int already_acked = 0;
    for (int i = 0; i < MAX_REPLICAS; i++) {
        if (server->replication_info->replicas_fd[i] != -1 && 
            server->replication_info->replica_ack_offsets[i] >= current_offset) {
            already_acked++;
        }
    }
    
    if (already_acked >= expected_replicas) {
        char response[32];
        sprintf(response, ":%d\r\n", already_acked);
        return strdup(response);
    }
    
    // Set up non-blocking wait
    server->pending_wait.client = (client_t *)client;
    server->pending_wait.expected_replicas = expected_replicas;
    server->pending_wait.target_offset = current_offset;
    server->pending_wait.start_time = get_current_time_ms();
    server->pending_wait.timeout_ms = timeout_ms;
    server->pending_wait.active = 1;
    
    // Send GETACK to all replicas
    char getack_cmd[] = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    for (int i = 0; i < MAX_REPLICAS; i++) {
        if (server->replication_info->replicas_fd[i] != -1) {
            send(server->replication_info->replicas_fd[i], getack_cmd, strlen(getack_cmd), MSG_NOSIGNAL);
            printf("Sent GETACK to replica fd=%d\n", server->replication_info->replicas_fd[i]);
        }
    }
    
    return NULL;
}

static int create_rdb_snapshot(redis_db_t *db)
{
    int fd = open(RDB_TEMP_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Failed to create RDB file");
        return -1;
    }

    io_buffer buffer;
    buffer_init_with_fd(&buffer, fd);
    
    ssize_t bytes_written = rdb_save_database(&buffer, db);
    if (bytes_written == -1) {
        perror("Failed to save RDB database");
        close(fd);
        unlink(RDB_TEMP_FILE);
        return -1;
    }
    
    // Flush any remaining buffered data
    if (!buffer_flush(&buffer)) {
        perror("Failed to flush RDB buffer");
        close(fd);
        unlink(RDB_TEMP_FILE);
        return -1;
    }
    
    printf("RDB snapshot created successfully: %zd bytes written to %s\n", bytes_written, RDB_TEMP_FILE);
    return fd;
}

static int send_rdb_file_to_client(int client_fd, const char *rdb_path)
{
    long file_size = get_file_size_stat(rdb_path);
    if (file_size == -1) {
        fprintf(stderr, "Failed to get RDB file size\n");
        return -1;
    }
    
    char header[64];
    int header_len = snprintf(header, sizeof(header), "$%ld\r\n", file_size);
    
    if (write(client_fd, header, header_len) != header_len) {
        perror("Failed to send RDB header to client");
        return -1;
    }
    
    // Open RDB file for reading
    FILE *rdb_file = fopen(rdb_path, "rb");
    if (!rdb_file) {
        perror("Failed to open RDB file for reading");
        return -1;
    }
    
    // Send file contents in chunks
    char file_buffer[8192];
    size_t bytes_read;
    
    while ((bytes_read = fread(file_buffer, 1, sizeof(file_buffer), rdb_file)) > 0) {
        ssize_t bytes_written = write(client_fd, file_buffer, bytes_read);
        if (bytes_written != (ssize_t)bytes_read) {
            perror("Failed to send RDB data to client");
            fclose(rdb_file);
            return -1;
        }
    }
    
    fclose(rdb_file);
    
    printf("Successfully sent RDB file to replica: %zu bytes\n");
    return 0;
}

static int rename_rdb_file(const char *temp_path, const char *main_path)
{
    // First, backup existing main RDB file if it exists
    char backup_path[256];
    snprintf(backup_path, sizeof(backup_path), "%s.bak", main_path);
    
    // Check if main file exists
    if (access(main_path, F_OK) == 0) {
        // Remove old backup if it exists
        if (access(backup_path, F_OK) == 0) {
            if (unlink(backup_path) != 0) {
                perror("Warning: Failed to remove old backup file");
            }
        }
        
        // Create backup of current main file
        if (rename(main_path, backup_path) != 0) {
            perror("Warning: Failed to backup existing RDB file");
            // Continue anyway, but warn user
        } else {
            printf("Backed up existing %s to %s\n", main_path, backup_path);
        }
    }
    
    // Rename temp file to main file
    if (rename(temp_path, main_path) != 0) {
        perror("Failed to rename temp RDB file to main file");
        
        // Try to restore backup if rename failed and backup exists
        if (access(backup_path, F_OK) == 0) {
            if (rename(backup_path, main_path) == 0) {
                printf("Restored backup file to %s\n", main_path);
            } else {
                fprintf(stderr, "Critical: Failed to restore backup file!\n");
            }
        }
        return -1;
    }
    
    // Successfully renamed, remove backup after a successful operation
    if (access(backup_path, F_OK) == 0) {
        if (unlink(backup_path) != 0) {
            perror("Warning: Failed to remove backup file after successful rename");
        } else {
            printf("Removed backup file %s after successful RDB update\n", backup_path);
        }
    }
    
    return 0;
}

static long get_file_size_stat(const char *filepath)
{
    struct stat st;
    if (stat(filepath, &st) == 0) {
        return st.st_size;
    }
    perror("Failed to stat file");
    return -1;
}

int add_replica(redis_server_t *server, int replica_fd) {
    if (!server || !server->replication_info || server->replication_info->role != MASTER) {
        return -1;
    }
    
    replication_info_t *repl_info = server->replication_info;
    
    // Check if already registered
    for (int i = 0; i < repl_info->connected_slaves; i++) {
        if (repl_info->replicas_fd[i] == replica_fd) {
            return 0;  // Already registered
        }
    }
    
    // Find empty slot
    for (int i = 0; i < MAX_REPLICAS; i++) {
        if (repl_info->replicas_fd[i] == -1) {  // Empty slot
            repl_info->replicas_fd[i] = replica_fd;
            repl_info->connected_slaves++;
            printf("Added replica fd %d, total replicas: %d\n", 
                   replica_fd, repl_info->connected_slaves);
            return 0;
        }
    }
    
    return -1;  // No empty slots
}


char *handle_config_get_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (argc < 3) {
        return strdup("-ERR wrong number of arguments for 'config get' command\r\n");
    }
    
    if (strcmp(args[1], "get") == 0) {
        char *param = args[2];
        
        char **response_args = malloc(2 * sizeof(char*));
        if (!response_args) {
            return strdup(RESP_MEMORY_ERROR);
        }
        
        response_args[0] = strdup(param);  
        
        if (strcmp(param, "dir") == 0) {
            response_args[1] = strdup(server->rdb_dir);  
        }
        else if (strcmp(param, "dbfilename") == 0) {
            response_args[1] = strdup(server->rdb_dir);  
        }
        else {
            free(response_args[0]);
            free(response_args);
            return encode_resp_array(NULL, 0);  
        }
        
        char *result = encode_resp_array(response_args, 2);
        
        free(response_args[0]);
        free(response_args[1]);
        free(response_args);
        
        return result;
    }
    
    return strdup("-ERR unknown CONFIG subcommand\r\n");
}

char *handle_keys_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (argc != 2) {
        return strdup("-ERR wrong number of arguments for 'keys' command\r\n");
    }
    
    char *pattern = args[1];
    
    // For now, only support "*" pattern (all keys)
    if (strcmp(pattern, "*") != 0) {
        // For unsupported patterns, return empty array
        return strdup("*0\r\n");
    }
    
    // Get all keys from the database
    hash_table_iterator_t *iter = hash_table_iterator_create(server->db->dict);
    if (!iter) {
        return strdup("*0\r\n");
    }
    
    // Count keys first
    int key_count = 0;
    char *key;
    void *value;
    
    while (hash_table_iterator_next(iter, &key, &value)) {
        key_count++;
    }
    
    if (key_count == 0) {
        hash_table_iterator_destroy(iter);
        return strdup("*0\r\n");
    }
    
    // Collect all keys
    char **keys_array = malloc(key_count * sizeof(char*));
    if (!keys_array) {
        hash_table_iterator_destroy(iter);
        return strdup("-ERR out of memory\r\n");
    }
    
    // Reset iterator and collect keys
    hash_table_iterator_destroy(iter);
    iter = hash_table_iterator_create(server->db->dict);
    
    int i = 0;
    while (hash_table_iterator_next(iter, &key, &value) && i < key_count) {
        keys_array[i] = key;  // Use the key directly from the hash table
        i++;
    }
    
    // Encode as RESP array
    char *response = encode_resp_array(keys_array, i);
    
    // Cleanup
    free(keys_array);
    hash_table_iterator_destroy(iter);
    
    return response;
}

char *handle_subscribe_command(redis_server_t *server, char **args, int argc, void *client)
{
    if (!server || !args || argc < 2 || !client) {
        return strdup("-ERR invalid arguments\r\n");
    }

    char *channel_name = args[1];
    client_t *c = (client_t *)client;
    
    // Get or create channel subscriber list
    redis_object_t *list_obj = hash_table_get(server->channels_map, channel_name);
    if (list_obj == NULL) {
        list_obj = redis_object_create_list();
        hash_table_set(server->channels_map, channel_name, list_obj);
    }
    
    redis_list_t *list = list_obj->ptr;
    
    // Check if client is already subscribed
    list_node_t *node = list->head;
    int already_subscribed = 0;
    while (node) {
        client_t *cur = (client_t *)node->data;
        if (cur == c) {
            already_subscribed = 1;
            break;
        }
        node = node->next;
    }
    
    // Add client if not already subscribed
    if (!already_subscribed) {
        c->sub_mode = 1;
        c->subscribed_channels++;
        list_rpush(list, c);
    }
    
    // Build proper RESP response
    // Format: *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
    int channel_len = strlen(channel_name);
    int response_size = 64 + channel_len;  // Buffer for response
    char *response = malloc(response_size);
    if (!response) {
        return strdup("-ERR out of memory\r\n");
    }
    
    snprintf(response, response_size, 
             "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
             channel_len, channel_name, c->subscribed_channels);
    
    return response;
}

static int is_pubsub_command(const char *command) {
    for (int i = 0; pubsub_allowed_commands[i] != NULL; i++) {
        if (strcmp(command, pubsub_allowed_commands[i]) == 0) {
            return 1;  
        }
    }
    return 0;  
}