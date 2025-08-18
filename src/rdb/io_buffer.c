#include "io_buffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void buffer_init_with_fd(io_buffer *b, int fd){
    b->fd = fd;
    b->buffered = 0;
    b->max_buffer = sizeof(b->buffer);
}

size_t buffer_write(io_buffer *b, const void *d, size_t len){
    if (b->buffered + len > b->max_buffer) {
        buffer_flush(b);
        return write(b->fd, d, len) == len;
    }
    memcpy(b->buffer + b->buffered, d, len);
    b->buffered += len;
    return 1;
}

int buffer_flush(io_buffer *b) {
    if (b->buffered > 0) {
        if (write(b->fd, b->buffer, b->buffered) != b->buffered)
            return 0;
        b->buffered = 0;  
    }
    return 1;
}
