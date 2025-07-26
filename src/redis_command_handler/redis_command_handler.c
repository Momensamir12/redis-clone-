#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include "../resp_praser/resp_parser.h"
#include "redis_command_handler.h"
#include "../redis_db/redis_db.h"

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
            free(args);  // Free the argument
        }
    }
    else if (strcmp(command, "ping") == 0) {  
        response = encode_simple_string("PONG");
    }
    else if (strcmp(command, "set")){
        char *key = parse_resp_array(resp_buffer);
        char *value = parse_resp_array(resp_buffer);
        handle_set_command(db, key, value);
        response = encode_simple_string("OK");
    }
    else if (strcmp(command, "get")){
      char *key = parse_resp_array(buffer);
      response = handle_get_command(db, key);
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

void handle_set_command(redis_db_t *db, const char *key, const char *value) {
    redis_object_t *obj = malloc(sizeof(redis_object_t));
    obj->type = REDIS_STRING;
    obj->ptr = strdup(value);  
    
    hash_table_set(db->dict, key, obj);
}

char* handle_get_command(redis_db_t *db, const char *key) {
    void *value = hash_table_get(db->dict, key);
    if (!value) {
        return encode_bulk_string("-1");  
    }
    
    redis_object_t *obj = (redis_object_t *)value;
    
    if (obj->type != REDIS_STRING) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    
    char *str = (char *)obj->ptr;
    
    char *response = encode_bulk_string(str);
    
    return response;
}
