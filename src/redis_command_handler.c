#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include "resp_parser.h"
#include "redis_command_handler.h"

char *handle_command(char *buffer)
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
    free(array_count);  // Free the array count string
    
    // Parse the command
    char *command = parse_resp_array(resp_buffer);
    if (!command) {
        free(resp_buffer);
        return NULL;
    }
    
    char *response = NULL;
    
    // strcmp returns 0 when equal!
    if (strcmp(command, "echo") == 0) {  // Fixed comparison
        char *args = parse_resp_array(resp_buffer);
        if (args) {
            response = encode_bulk_string(args);
            free(args);  // Free the argument
        }
    }
    else if (strcmp(command, "ping") == 0) {  // Fixed comparison
        response = encode_simple_string("PONG");
    }
    else {
        // Unknown command
        response = malloc(32);
        if (response) {
            sprintf(response, "-ERR unknown command\r\n");
        }
    }
    
    free(command);  // Free the command string
    free(resp_buffer);
    return response;
}
