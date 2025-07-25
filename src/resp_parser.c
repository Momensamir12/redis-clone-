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
        return result;  // Return early for array count
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

    // Calculate space: "$" + num digits of len + "\r\n" + str + "\r\n" + null terminator
    int total = 1 + 10 + 2 + len + 2 + 1;  // oversize buffer to be safe
    char *result = malloc(total);
    if (!result) return NULL;

    sprintf(result, "$%d\r\n%s\r\n", len, str);
    return result;
}

char* encode_simple_string(const char *str) {
    int len = strlen(str);

    // Allocate memory for '+' + str + "\r\n" + '\0'
    char *result = malloc(len + 4);  // 1 for '+', 2 for \r\n, 1 for \0
    if (!result) return NULL;

    // Format: +<string>\r\n
    sprintf(result, "+%s\r\n", str);

    return result;
}