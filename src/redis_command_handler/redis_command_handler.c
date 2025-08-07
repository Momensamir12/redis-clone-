#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
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

#define NULL_RESP_VALUE "$-1\r\n"

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
    {NULL, NULL, 0, 0} // Sentinel
};

void init_command_table(void)
{
    if (command_table != NULL)
        return; // Already initialized

    command_table = hash_table_create(32); 

    for (int i = 0; commands[i].name != NULL; i++)
    {
        // Store pointer to command struct
        hash_table_set(command_table, commands[i].name, &commands[i]);

        // Also store lowercase version for case-insensitive lookup
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
    free(cmd_lower);

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

    // Call handler
    char *response = cmd->handler(server, args, argc, client);

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
    (void)client;
    if (argc == 2)
    {
        return encode_bulk_string(args[1]);
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
            return strdup("-ERR unknown error\r\n");
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