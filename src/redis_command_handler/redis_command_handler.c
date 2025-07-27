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
char *handle_command(redis_db_t *db, char *buffer) {
    // Ensure command table is initialized
    if (command_table == NULL) {
        init_command_table();
    }
    
    resp_buffer_t *resp_buffer = calloc(1, sizeof(resp_buffer_t));
    if (!buffer || !resp_buffer) return NULL;
    
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
    
    // Call handler
    char *response = cmd->handler(db, args, argc);
    
    free_command_args(args, argc);
    free(resp_buffer);
    return response;
}

// Command handlers
char *handle_echo_command(redis_db_t *db, char **args, int argc) {
    (void)db;  // Unused
    (void)argc; // Already validated
    return encode_bulk_string(args[1]);
}

char *handle_ping_command(redis_db_t *db, char **args, int argc) {
    (void)db;  // Unused
    if (argc == 2) {
        return encode_bulk_string(args[1]);
    }
    return encode_simple_string("PONG");
}

char *handle_set_command(redis_db_t *db, char **args, int argc) {
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
            // Convert seconds to milliseconds
            int seconds = atoi(args[i + 1]);
            char ms_buf[32];
            sprintf(ms_buf, "%d", seconds * 1000);
            expiry_ms = ms_buf;
            i += 2;
        } else {
            return strdup("-ERR syntax error\r\n");
        }
    }
    
    // Create object
    redis_object_t *obj = redis_object_create_string(value);
    
    if (expiry_ms) {
        set_expiry_ms(obj, atoi(expiry_ms));
    }
    
    hash_table_set(db->dict, strdup(key), obj);
    return encode_simple_string("OK");
}

char *handle_get_command(redis_db_t *db, char **args, int argc) {
    (void)argc;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
    if (!obj) {
        return strdup(NULL_RESP_VALUE);
    }
    
    if (is_expired(obj)) {
        redis_object_destroy(obj);
        hash_table_delete(db->dict, key);
        return strdup(NULL_RESP_VALUE);
    }
    
    if (obj->type != REDIS_STRING) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    return encode_bulk_string((char *)obj->ptr);
}

char *handle_rpush_command(redis_db_t *db, char **args, int argc) {
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
    if (!obj) {
        obj = redis_object_create_list();
        hash_table_set(db->dict, strdup(key), obj);
    } else if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    
    // Push all values
    for (int i = 2; i < argc; i++) {
        list_rpush(list, strdup(args[i]));
    }
    
    // Return the length as integer
    char response[32];
    sprintf(response, ":%zu\r\n", list_length(list));
    return strdup(response);
}

char *handle_lpush_command(redis_db_t *db, char **args, int argc) {
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
    if (!obj) {
        obj = redis_object_create_list();
        hash_table_set(db->dict, strdup(key), obj);
    } else if (obj->type != REDIS_LIST) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    
    for (int i = 2; i < argc; i++) {
        list_lpush(list, strdup(args[i]));
    }
    
    char response[32];
    sprintf(response, ":%zu\r\n", list_length(list));
    return strdup(response);
}

char *handle_llen_command(redis_db_t *db, char **args, int argc) {
    (void)argc;
    char *key = args[1];
    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
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

char *handle_rpop_command(redis_db_t *db, char **args, int argc) {
    (void)argc;
    char *key = args[1];

    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
    if (!obj || obj->type != REDIS_LIST) {
        return strdup(NULL_RESP_VALUE);
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    char *value = (char *)list_rpop(list);
    
    if (!value) {
        return strdup(NULL_RESP_VALUE);
    }
    
    char *response = encode_bulk_string(value);
    free(value);  // Free the popped value
    return response;
}

char *handle_lpop_command(redis_db_t *db, char **args, int argc) {
    (void)argc;
    char *key = args[1];
    size_t count = 0;
    if(argc >= 2){
      count = atoi(args[2]);
    }
    printf("%s%d\n","count of elements is",count);
    redis_object_t *obj = (redis_object_t *)hash_table_get(db->dict, key);
    
    if (!obj || obj->type != REDIS_LIST) {
        return strdup(NULL_RESP_VALUE);
    }
    
    redis_list_t *list = (redis_list_t *)obj->ptr;
    char **value = calloc(count, sizeof(char*));
    if(count <= 0)
      return NULL_RESP_VALUE;
    int i = 0;
    int actual_count = 0;  
    while(count--){
      value[i++] = (char *)list_lpop(list);
      if(value[i]){
        actual_count++;
      }
      else
        break;
    }  
    
    if (actual_count == 0) {
        free(value);
        return strdup("*0\r\n");
    }
    
    char *response = encode_resp_array(value, actual_count);
    free(value);
    return response;
}

char *handle_lrange_command(redis_db_t *db, char **args, int argc) {
    if (argc != 4) {
        return strdup("-ERR wrong number of arguments for 'lrange' command\r\n");
    }
    
    char *key = args[1];
    int start = atoi(args[2]);
    int stop = atoi(args[3]);
    
    redis_object_t *obj = hash_table_get(db->dict, key);
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