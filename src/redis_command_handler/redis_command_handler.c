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
#define NULL_RESP_VALUE "$-1\r\n"

// Global command hash table
static hash_table_t *command_table = NULL;

// Command definitions
static redis_command_t commands[] = {
    {"echo",  handle_echo_command,  2,  2},
    {"ping",  handle_ping_command,  1,  2},
    {"set",   handle_set_command,   3, -1},
    {"get",   handle_get_command,   2,  2},
    {"rpush", handle_rpush_command, 3, -1},
    {"lpush", handle_lpush_command, 3, -1},
    {"llen",  handle_llen_command,  2,  2},
    {"rpop",  handle_rpop_command,  2,  2},
    {"lpop",  handle_lpop_command,  2,  3},
    {"lrange", handle_lrange_command, 4, 4},
    {"blpop", handle_blpop_command, 3, -1},
    {"type", handle_type_command, 2, 2},
    {NULL, NULL, 0, 0}  // Sentinel
};

void init_command_table(void) {
    if (command_table != NULL) return;  // Already initialized
    
    command_table = hash_table_create(32);  // Small table, we don't have many commands
    
    for (int i = 0; commands[i].name != NULL; i++) {
        // Store pointer to command struct
        hash_table_set(command_table, commands[i].name, &commands[i]);
        
        // Also store lowercase version for case-insensitive lookup
        char *lower = strdup(commands[i].name);
        for (char *p = lower; *p; p++) {
            *p = tolower(*p);
        }
        hash_table_set(command_table, lower, &commands[i]);
        free(lower);
    }
}

// Parse all arguments into an array
static char **parse_command_args(resp_buffer_t *resp_buffer, int *argc) {
    char *array_count = parse_resp_array(resp_buffer);
    if (!array_count) return NULL;
    
    *argc = atoi(array_count);
    free(array_count);
    
    if (*argc <= 0) return NULL;
    
    char **args = calloc(*argc, sizeof(char*));
    if (!args) return NULL;
    
    for (int i = 0; i < *argc; i++) {
        args[i] = parse_resp_array(resp_buffer);
        if (!args[i]) {
            for (int j = 0; j < i; j++) {
                free(args[j]);
            }
            free(args);
            return NULL;
        }
    }
    
    return args;
}

// Free argument array
static void free_command_args(char **args, int argc) {
    if (!args) return;
    for (int i = 0; i < argc; i++) {
        if (args[i]) free(args[i]);
    }
    free(args);
}

// Main command handler
char *handle_command(redis_server_t *server, char *buffer, void *client) {
    if (!server || !buffer) return NULL;
    
    resp_buffer_t *resp_buffer = calloc(1, sizeof(resp_buffer_t));
    if (!resp_buffer) return NULL;
    
    resp_buffer->buffer = buffer;
    resp_buffer->size = strlen(buffer);
    resp_buffer->pos = 0;
    
    int argc;
    char **args = parse_command_args(resp_buffer, &argc);
    if (!args || argc < 1) {
        free(resp_buffer);
        return strdup("-ERR protocol error\r\n");
    }
    
    // Convert command to lowercase for lookup
    char *cmd_lower = strdup(args[0]);
    for (char *p = cmd_lower; *p; p++) {
        *p = tolower(*p);
    }
    
    // Look up command
    redis_command_t *cmd = (redis_command_t *)hash_table_get(command_table, cmd_lower);
    free(cmd_lower);
    
    if (!cmd) {
        char response[256];
        sprintf(response, "-ERR unknown command '%s'\r\n", args[0]);
        free_command_args(args, argc);
        free(resp_buffer);
        return strdup(response);
    }
    
    // Validate argument count
    if (argc < cmd->min_args || (cmd->max_args != -1 && argc > cmd->max_args)) {
        char response[256];
        sprintf(response, "-ERR wrong number of arguments for '%s' command\r\n", cmd->name);
        free_command_args(args, argc);
        free(resp_buffer);
        return strdup(response);
    }
    
    // Call handler with server context
    char *response = cmd->handler(server, args, argc, client);
    
    free_command_args(args, argc);
    free(resp_buffer);
    return response;
}

// Command handlers
static void check_blocked_clients_for_key(redis_server_t *server, const char *key) {
    if (!server || !server->blocked_clients || !key) return;
    
    list_node_t *node = server->blocked_clients->head;
    while (node) {
        list_node_t *next = node->next;
        client_t *blocked_client = (client_t *)node->data;
        
        if (blocked_client->blocked_key && 
            strcmp(blocked_client->blocked_key, key) == 0) {
            
            // Check if key now has data
            redis_object_t *obj = hash_table_get(server->db->dict, key);
            if (obj && obj->type == REDIS_LIST) {
                redis_list_t *list = (redis_list_t *)obj->ptr;
                if (list_length(list) > 0) {
                    // Pop value
                    char *value = (char *)list_lpop(list);
                    
                    if (value) {
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
        }
        node = next;
    }
}

// Update command handlers to use server context
char *handle_echo_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)server;
    (void)client;
    (void)argc;
    return encode_bulk_string(args[1]);
}

char *handle_ping_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)server;
    (void)client;
    if (argc == 2) {
        return encode_bulk_string(args[1]);
    }
    return encode_simple_string("PONG");
}

char *handle_set_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)client;
    char *key = args[1];
    char *value = args[2];
    char *expiry_ms = NULL;
    
    // Parse optional arguments
    int i = 3;
    while (i < argc) {
        if (strcasecmp(args[i], "px") == 0 && i + 1 < argc) {
            expiry_ms = args[i + 1];
            i += 2;
        } else if (strcasecmp(args[i], "ex") == 0 && i + 1 < argc) {
            int seconds = atoi(args[i + 1]);
            char ms_buf[32];
            sprintf(ms_buf, "%d", seconds * 1000);
            expiry_ms = ms_buf;
            i += 2;
        } else {
            return strdup("-ERR syntax error\r\n");
        }
    }
    
    redis_object_t *obj = redis_object_create_string(value);
    
    if (expiry_ms) {
        set_expiry_ms(obj, atoi(expiry_ms));
    }
    
    hash_table_set(server->db->dict, strdup(key), obj);
    return encode_simple_string("OK");
}

char *handle_get_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)argc;
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj) {
        return strdup(NULL_RESP_VALUE);
    }
    
    if (is_expired(obj)) {
        redis_object_destroy(obj);
        hash_table_delete(server->db->dict, key);
        return strdup(NULL_RESP_VALUE);
    }
    
    if (obj->type != REDIS_STRING) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    return encode_bulk_string((char *)obj->ptr);
}

// LPUSH with blocking client notification
// In redis_command_handler.c - Fix RPUSH
char *handle_rpush_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj) {
        obj = redis_object_create_list();
        hash_table_set(server->db->dict, strdup(key), obj);
    } else if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    
    // Push all values
    for (int i = 2; i < argc; i++) {
        list_rpush(list, strdup(args[i]));
    }
    
    // Get the length BEFORE checking blocked clients
    size_t list_len = list_length(list);
    
    // Now check blocked clients (they might consume the values)
    check_blocked_clients_for_key(server, key);
    
    char response[32];
    sprintf(response, ":%zu\r\n", list_len);
    return strdup(response);
}

// Same fix for LPUSH
char *handle_lpush_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)client;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj) {
        obj = redis_object_create_list();
        hash_table_set(server->db->dict, strdup(key), obj);
    } else if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    
    // Push all values
    for (int i = 2; i < argc; i++) {
        list_lpush(list, strdup(args[i]));
    }
    
    // Get the length BEFORE checking blocked clients
    size_t list_len = list_length(list);
    
    // Check if any clients are blocked on this key
    check_blocked_clients_for_key(server, key);
    
    // Return the original length
    char response[32];
    sprintf(response, ":%zu\r\n", list_len);
    return strdup(response);
}

// BLPOP implementation
// In handle_blpop_command - Fix timeout parsing
// In handle_blpop_command - Simple fix without ceil()
char *handle_blpop_command(redis_server_t *server, char **args, int argc, void *client) {
    if (!client || !server) {
        return strdup("-ERR internal error\r\n");
    }
    
    client_t *c = (client_t *)client;
    
    // Parse timeout as float
    double timeout_float = atof(args[argc - 1]);
    int timeout;
    
    // Handle fractional seconds
    if (timeout_float == 0.0) {
        timeout = 0;  // Wait forever
    } else if (timeout_float > 0.0 && timeout_float < 1.0) {
        timeout = 1;  // Minimum 1 second for fractional values
    } else {
        timeout = (int)timeout_float;  // Truncate to seconds
        // Add 1 if there's a fractional part
        if (timeout_float > timeout) {
            timeout++;
        }
    }
    
    // Check if client is already blocked
    if (c->is_blocked) {
        return strdup("-ERR client already blocked\r\n");
    }
    
    // Try each key (except last arg which is timeout)
    for (int i = 1; i < argc - 1; i++) {
        char *key = args[i];
        redis_object_t *obj = hash_table_get(server->db->dict, key);
        
        if (obj && obj->type == REDIS_LIST) {
            redis_list_t *list = (redis_list_t *)obj->ptr;
            if (list_length(list) > 0) {
                // Can pop immediately
                char *value = (char *)list_lpop(list);
                if (value) {
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
    
    // No keys have values - block the client
    client_block(c, args[1], timeout);
    add_client_to_list(server->blocked_clients, c);
    
    printf("Client fd=%d blocked on key '%s' with timeout %d seconds\n", 
           c->fd, args[1], timeout);
    
    return NULL;  // No response - client is blocked
}

// Other command handlers remain similar, just update signature...
char *handle_llen_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)argc;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj) {
        return strdup(":0\r\n");
    }
    
    if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    char response[32];
    sprintf(response, ":%zu\r\n", list_length(list));
    return strdup(response);
}

char *handle_rpop_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)argc;
    (void)client;
    char *key = args[1];

    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);
    
    if (!obj || obj->type != REDIS_LIST) {
        return strdup(NULL_RESP_VALUE);
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    char *value = (char *)list_rpop(list);
    
    if (!value) {
        return strdup(NULL_RESP_VALUE);
    }
    
    char *response = encode_bulk_string(value);
    free(value);
    return response;
}

char *handle_lpop_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)client;
    char *key = args[1];
    size_t count = 0;
    if (argc >= 3) {
        count = atoi(args[2]);
    }

    redis_object_t *obj = (redis_object_t *)hash_table_get(server->db->dict, key);

    if (!obj || obj->type != REDIS_LIST) {
        return strdup(NULL_RESP_VALUE);
    }

    redis_list_t *list = (redis_list_t *)obj->ptr;
    
    if (count == 0) count = 1;
    
    char **values = calloc(count, sizeof(char *));
    int actual_count = 0;

    for (int i = 0; i < count; i++) {
        values[i] = (char *)list_lpop(list);
        if (values[i]) {
            actual_count++;
        } else {
            break;
        }
    }
    
    if (actual_count == 0) {
        free(values);
        return strdup("*0\r\n");
    }
    
    char *response;
    if (argc < 3 && actual_count == 1) {
        // Single value response for LPOP without count
        response = encode_bulk_string(values[0]);
        free(values[0]);
    } else {
        // Array response for LPOP with count
        response = encode_resp_array(values, actual_count);
        for (int i = 0; i < actual_count; i++) {
            free(values[i]);
        }
    }
    
    free(values);
    return response;
}

char *handle_lrange_command(redis_server_t *server, char **args, int argc, void *client) {
    (void)client;
    if (argc != 4) {
        return strdup("-ERR wrong number of arguments for 'lrange' command\r\n");
    }
    
    char *key = args[1];
    int start = atoi(args[2]);
    int stop = atoi(args[3]);
    
    redis_object_t *obj = hash_table_get(server->db->dict, key);
    if (!obj) {
        return strdup("*0\r\n");
    }
    
    if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    int count;
    char **values = list_range(list, start, stop, &count);
    
    if (!values) {
        return strdup("*0\r\n");
    }
    
    char *response = encode_resp_array(values, count);
    free(values);  
    
    return response;
}

// In redis_command_handler.c - Update check_blocked_clients_timeout
void check_blocked_clients_timeout(redis_server_t *server) {
    if (!server || !server->blocked_clients) return;
    
    time_t now = time(NULL);
    list_node_t *node = server->blocked_clients->head;
    
    printf("Checking %zu blocked clients for timeout\n", list_length(server->blocked_clients));
    
    while (node) {
        list_node_t *next = node->next;
        client_t *client = (client_t *)node->data;
        
        // Only check timeout if block_timeout > 0 (0 means wait forever)
        if (client->block_timeout > 0 && client->block_timeout <= now) {
            printf("Client fd=%d timed out\n", client->fd);
            
            // Send nil response for timeout
            const char *nil_response = "*-1\r\n";
            send(client->fd, nil_response, strlen(nil_response), MSG_NOSIGNAL);
            
            // Unblock client
            client_unblock(client);
            remove_client_from_list(server->blocked_clients, client);
        }
        else if (client->blocked_key) {
            redis_object_t *obj = hash_table_get(server->db->dict, client->blocked_key);
            if (obj && obj->type == REDIS_LIST) {
                redis_list_t *list = (redis_list_t *)obj->ptr;
                if (list_length(list) > 0) {
                    // Pop value
                    char *value = (char *)list_lpop(list);
                    if (value) {
                        // Send response
                        char response[1024];
                        snprintf(response, sizeof(response), 
                                "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                                strlen(client->blocked_key), client->blocked_key, 
                                strlen(value), value);
                        send(client->fd, response, strlen(response), MSG_NOSIGNAL);
                        
                        // Unblock client
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

char *handle_type_command (redis_server_t *server, char **args, int argc, void *client)
{
    char *key = args[1];
    void *value = hash_table_get(server->db->dict, key);
    if(!value)
    {
        return encode_simple_string("none");
    }
    redis_object_t *obj = (redis_object_t *)value;
    char *response = encode_simple_string(redis_type_to_string(obj->type));
    
    free(value);
    return response;
}