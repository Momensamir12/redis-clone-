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

#define NULL_RESP_VALUE "$-1\r\n"

char *handle_command(redis_db_t *db, char * buffer)
{
    resp_buffer_t *resp_buffer = calloc(1, sizeof(resp_buffer_t));
    if(!buffer || !resp_buffer)
        return NULL;

    resp_buffer->buffer = buffer;
    resp_buffer->size = strlen(buffer);
    resp_buffer->pos = 0;
    
    // Parse the array count
    char *array_count = parse_resp_array(resp_buffer);
    if (!array_count) {
        free(resp_buffer);
        return NULL;
    }
    int len = atoi(array_count);
    free(array_count); 
    
    char *command = parse_resp_array(resp_buffer);
    if (!command) {
        free(resp_buffer);
        return NULL;
    }
    
    char *response = NULL;
    
    // strcmp returns 0 when equal!
    if (strcmp(command, "echo") == 0) {  
        char *args = parse_resp_array(resp_buffer);
        if (args) {
            response = encode_bulk_string(args);
            free(args);
        }
    }
    else if (strcmp(command, "ping") == 0) {  
        response = encode_simple_string("PONG");
    }
    else if (strcmp(command, "set") == 0){
        char *key = parse_resp_array(resp_buffer);
        char *value = parse_resp_array(resp_buffer);
        
        if (!key || !value) {
            response = encode_simple_string("ERR wrong number of arguments");
        } else {
            // Check if there are more arguments
            char *next_arg = NULL;
            if (len > 3) {  // SET key value [px time]
                next_arg = parse_resp_array(resp_buffer);
            }
            
            if (next_arg && strcasecmp(next_arg, "px") == 0) {
                char *expiry = parse_resp_array(resp_buffer);
                if (expiry) {
                    handle_set_command(db, key, value, expiry);
                    free(expiry);
                } else {
                    response = encode_simple_string("ERR syntax error");
                }
                free(next_arg);
            } else {
                handle_set_command(db, key, value, NULL);
                if (next_arg) free(next_arg);
            }
            
            if (!response) {
                response = encode_simple_string("OK");
            }
        }
        
        if (key) free(key);
        if (value) free(value);
    }
    else if (strcmp(command, "get") == 0){
        char *key = parse_resp_array(resp_buffer);
        if (key) {
            response = handle_get_command(db, key);
            free(key);
        } else {
            response = encode_simple_string("ERR wrong number of arguments");
        }
    }
    else {
        // Unknown command
        response = malloc(32);
        if (response) {
            sprintf(response, "-ERR unknown command\r\n");
        }
    }
    
    free(command);  
    free(resp_buffer);
    return response;
}

void handle_set_command(redis_db_t *db, char *key, char *value, char *expiry) {
    redis_object_t *obj = malloc(sizeof(redis_object_t));
    obj->type = REDIS_STRING;
    obj->ptr = strdup(value);
    obj->expiry = 0;  
    
    if(expiry != NULL){
        set_expiry_ms(obj, atoi(expiry));
    }
    
    hash_table_set(db->dict, strdup(key), obj); 
}

char* handle_get_command(redis_db_t *db, char *key) {
    void *value = hash_table_get(db->dict, key);
    if (!value) {
        return strdup(NULL_RESP_VALUE);  
    }
    
    redis_object_t *obj = (redis_object_t *)value;
    if(is_expired(obj)) {
        // Remove expired key
        hash_table_delete(db->dict, key);
        redis_object_destroy(obj);
        return strdup(NULL_RESP_VALUE);
    }
    
    if (obj->type != REDIS_STRING) {
        return strdup("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    
    char *str = (char *)obj->ptr;
    char *response = encode_bulk_string(str);
    
    return response;
}