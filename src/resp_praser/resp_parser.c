#include "resp_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>

static void toLowerCase (char *str);
char* extract_until_delimiter(resp_buffer_t *resp_buffer) {
    const char *start = resp_buffer->buffer + resp_buffer->pos;
    const char *end = strstr(start, "\r\n"); // search starting from current pos
    if (!end) return NULL; // delimiter not found

    size_t len = end - start;
    char *result = malloc(len + 1);
    if (!result) return NULL;

    strncpy(result, start, len);
    result[len] = '\0';

    resp_buffer->pos += (len + 2); 

    return result;
}

char *parse_resp_array(resp_buffer_t *resp_buffer)
{
    if (!resp_buffer || resp_buffer->pos >= resp_buffer->size) {
        return NULL;
    }
    
    char *result = NULL;
    
    if (resp_buffer->buffer[resp_buffer->pos] == '*') {
        resp_buffer->pos += 1;
        result = extract_until_delimiter(resp_buffer);
        return result;  
    }
    
    if (resp_buffer->buffer[resp_buffer->pos] == '$') {
        result = parse_resp_bulk_string(resp_buffer); 
        if (result) {
            toLowerCase(result);  
        }
    }
    
    return result;
}

char *parse_resp_bulk_string(resp_buffer_t *resp_buffer) {
    if (resp_buffer->buffer[resp_buffer->pos] != '$') {
        return NULL;
    }
    
    resp_buffer->pos += 1;  
    
    char *len_str = extract_until_delimiter(resp_buffer);
    if (!len_str) {
        return NULL;
    }
    
    int len = atoi(len_str);
    free(len_str);
    
    if (resp_buffer->pos + len > resp_buffer->size) {
        return NULL;  
    }
    
    char *result = malloc(len + 1);
    if (!result) {
        return NULL;
    }
    
    memcpy(result, resp_buffer->buffer + resp_buffer->pos, len);
    result[len] = '\0';
    
    resp_buffer->pos += len + 2; 
    
    return result;
}

static void toLowerCase(char *str) {
    for (int i = 0; str[i]; i++) {
        str[i] = tolower(str[i]);
    }
}

char* encode_bulk_string(const char *str) {
    int len = strlen(str);

    int total = 1 + 10 + 2 + len + 2 + 1;  
    char *result = malloc(total);
    if (!result) return NULL;

    sprintf(result, "$%d\r\n%s\r\n", len, str);
    return result;
}

char* encode_simple_string(const char *str) {

    if(!str)
      return NULL;
    int len = strlen(str);

    char *result = malloc(len + 4);  
    if (!result) return NULL;

    sprintf(result, "+%s\r\n", str);

    return result;
}

char *encode_resp_array(char **args, int argc) {
    if (argc <= 0 || !args) {
        return strdup("*0\r\n");  // Empty array
    }
    
    size_t total_size = 0;
    
    char count_str[32];
    int count_len = sprintf(count_str, "*%d\r\n", argc);
    total_size += count_len;
    
    for (int i = 0; i < argc; i++) {
        if (args[i] == NULL) {
            total_size += 5;  
        } else {
            size_t len = strlen(args[i]);
            char len_str[32];
            int len_size = sprintf(len_str, "%zu", len);
            total_size += 1 + len_size + 2 + len + 2; 
        }
    }
    
    char *result = malloc(total_size + 1);  
    if (!result) return NULL;
    
    char *pos = result;
    
    memcpy(pos, count_str, count_len);
    pos += count_len;
    
    // Write each element as bulk string
    for (int i = 0; i < argc; i++) {
        if (args[i] == NULL) {
            memcpy(pos, "$-1\r\n", 5);
            pos += 5;
        } else {
            size_t len = strlen(args[i]);
            int written = sprintf(pos, "$%zu\r\n", len);
            pos += written;
            memcpy(pos, args[i], len);
            pos += len;
            memcpy(pos, "\r\n", 2);
            pos += 2;
        }
    }
    
    *pos = '\0';  
    return result;
}

char *encode_number(const char *str)
{
    if(!str)
      return NULL;
    int len = strlen(str);
    char *result = malloc(len + 4);
    sprintf(result, ":%s\r\n", str);

    return result;
}

