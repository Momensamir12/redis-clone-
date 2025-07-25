#ifndef RESP_PARSER_H
#define RESP_PARSER_H
#include <stddef.h>
#include <stdbool.h>
typedef enum {
    RESP_SIMPLE_STRING,
    RESP_ERROR,
    RESP_INTEGER,
    RESP_BULK_STRING,
    RESP_ARRAY,
    RESP_NULL
} resp_type_t;

typedef struct resp_value {
    resp_type_t type;
    union {
        char *string;           // For simple string, error, bulk string
        long long integer;      // For integer
        struct {
            struct resp_value **elements;
            size_t count;
        } array;               // For array
    } data;
} resp_value_t;


typedef struct resp_buffer {
    char * buffer;
    int pos;
    int size;
}resp_buffer_t;

char *parse_resp_array(resp_buffer_t *buffer);
char *parse_resp_bulk_string(resp_buffer_t *buffer);
char *encode_bulk_string(const char *str);

#endif