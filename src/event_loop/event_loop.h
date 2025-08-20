#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <sys/epoll.h>
#include <stdbool.h>

#define MAX_EVENTS 1024

typedef struct event_loop event_loop_t;

typedef void (*event_handler_t) (event_loop_t *event_loop, int fd, uint32_t events, void *data);

typedef struct event_loop {
    int epoll_fd;
    int timer_fd;
    bool running;
    struct epoll_event events[MAX_EVENTS];
    struct {
        event_handler_t handler;
        void *data;
    } handlers [MAX_EVENTS];
    void *server_data;
} event_loop_t;

event_loop_t* event_loop_create(void);
void event_loop_destroy(event_loop_t *event_loop);

int event_loop_add_fd(event_loop_t *event_loop, int fd, uint32_t events, 
                      event_handler_t handler, void *data);
int event_loop_remove_fd(event_loop_t *event_loop, int fd);

void event_loop_run(event_loop_t *event_loop);
void event_loop_stop(event_loop_t *event_loop);

int set_nonblocking(int fd);
int setup_timer_fd ();

#endif 
