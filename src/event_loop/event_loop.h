#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <sys/epoll.h>
#include <stdbool.h>

#define MAX_EVENTS 1024

typedef struct event_loop event_loop_t;

typedef void (*event_handler_t) (event_loop_t *event_loop, int fd, uint32_t events, void *data);

typedef struct event_loop {
    int epoll_fd;
    bool running;
    struct epoll_event events[MAX_EVENTS];
    struct {
        event_handler_t handler;
        void *data;
    } handlers [MAX_EVENTS];
} event_loop_t;

event_loop_t* event_loop_create(void);
void event_loop_destroy(event_loop_t *event_loop);

// Add/remove file descriptors
int event_loop_add_fd(event_loop_t *event_loop, int fd, uint32_t events, 
                      event_handler_t handler, void *data);
int event_loop_remove_fd(event_loop_t *event_loop, int fd);

// Run the event loop
void event_loop_run(event_loop_t *event_loop);
void event_loop_stop(event_loop_t *event_loop);

// Utility to set non-blocking
int set_nonblocking(int fd);

#endif // EVENT_LOOP_H
