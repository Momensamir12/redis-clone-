#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include "resp_parser.h"
#include "redis_command_handler.h"

char *handle_command (char *buffer)
{
    resp_buffer_t *resp_buffer = calloc(1, sizeof(resp_buffer_t));
    if(!buffer)
      return NULL;

    resp_buffer->buffer = buffer;
    resp_buffer->size = strlen(buffer);
    resp_buffer->pos = 0;
    int len = atoi(parse_resp_array(resp_buffer));
    char * command = parse_resp_array(resp_buffer);
    char * args = parse_resp_array(resp_buffer);
    char * response;
    if(strcmp(command, "echo"))
    {
        response = encode_bulk_string(args);
        free(resp_buffer);
        return response;
    }
    if(strcmp(command, "ping")){
        response = encode_simple_string("PONG");
        free(resp_buffer);
        return response;
    }
    
    return NULL;
}
