#ifndef IO_BUFFER_H
#define IO_BUFFER_H


#include <unistd.h>     // For write, read, close
#include <sys/types.h>  // For ssize_t, size_t
#include <errno.h>      // For errno
#include <string.h>     // For memcpy if needed
#include <stdlib.h>     // For malloc, free
#include <fcntl.h>      // For open flags


#define MAX_BUFFER_SIZE 1024 * 6

typedef struct {
    int fd;
    size_t buffered;
    size_t max_buffer;
    char buffer[MAX_BUFFER_SIZE];
} io_buffer;

void buffer_init_with_fd(io_buffer *b, int fd);
size_t buffer_write(io_buffer *b, const void *d, size_t len);
int buffer_flush(io_buffer *b);

#endif