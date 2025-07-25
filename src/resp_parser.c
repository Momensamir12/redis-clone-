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

char *parse_resp_bulk_string(resp_buffer_t *resp_buffer){
    resp_buffer->pos +=1;
    char * result;
    char * len = extract_until_delimiter(resp_buffer);
    if(!len){
        free(len);
        return NULL;
    }
    if(isalpha(resp_buffer->buffer[resp_buffer->pos])){
        result = extract_until_delimiter(resp_buffer);
        if(!result){
            free(result);
            return NULL;
        }
        return result;
    }
    return NULL;
}

char *parse_resp_array(resp_buffer_t *resp_buffer)
{
  char *result;
  if(resp_buffer->buffer[resp_buffer->pos] == '*'){
  resp_buffer->pos += 1;
  result = extract_until_delimiter(resp_buffer);
  if(!result){
    free(result);
    return NULL;
  }
  }
  if(resp_buffer->buffer[resp_buffer->pos] == '$'){
     result = parse_resp_bulk_string(resp_buffer); 
     toLowerCase(result);  
  }  
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