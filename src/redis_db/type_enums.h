#ifndef TYPE_ENUMS_H
#define TYPE_ENUMS_H

typedef enum {
    REDIS_STRING,
    REDIS_LIST,
    REDIS_HASH,
    REDIS_SET,
    REDIS_ZSET,
    REDIS_STREAM,
    REDIS_NUMBER
} redis_type_t;

#endif