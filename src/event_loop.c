#include "event_loop.h"
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

event_loop_t *event_loop_create(void)
{
    event_loop_t *event_loop = calloc(1, sizeof(event_loop_t));
    if (!event_loop)
        return NULL;

    event_loop->epoll_fd = epoll_create1(0);
    if (event_loop->epoll_fd < 0)
    {
        free(event_loop);
        return NULL;
    }
    event_loop->running = false;
    return event_loop;
}

void event_loop_destroy(event_loop_t *event_loop)
{
    if (!event_loop)
        return;

    if (event_loop->epoll_fd >= 0)
    {
        close(event_loop->epoll_fd);
    }

    free(event_loop);
}

int event_loop_add_fd(event_loop_t *event_loop, int fd, uint32_t events,
                      event_handler_t handler, void *data)
{
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = events;
    
    if (epoll_ctl(event_loop->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return -1;
    }
    
    event_loop->handlers[fd].handler = handler;
    event_loop->handlers[fd].data = data;
    
    return 0;  
}


int event_loop_remove_fd(event_loop_t *event_loop, int fd)
{
    if (!event_loop || fd < 0 || fd >= MAX_EVENTS)
    {
        return -1;
    }

    if (epoll_ctl(event_loop->epoll_fd, EPOLL_CTL_DEL, fd, NULL) < 0)
    {
        return -1;
    }

    event_loop->handlers[fd].handler = NULL;
    event_loop->handlers[fd].data = NULL;

    return 0;
}

void event_loop_run(event_loop_t *event_loop)
{
    if (!event_loop)
        return;

    event_loop->running = true;
    while (event_loop->running)
    {
        int ndfs = epoll_wait(event_loop->epoll_fd, event_loop->events, MAX_EVENTS, -1);
        for (int i = 0; i < ndfs; i++)
        {
            int fd = event_loop->events[i].data.fd;
            uint32_t events = event_loop->events[i].events;
            
            if (fd < MAX_EVENTS && event_loop->handlers[fd].handler) {
                event_loop->handlers[fd].handler(event_loop, fd, events, 
                                                event_loop->handlers[fd].data);  
            }
        }
    }
}

void event_loop_stop(event_loop_t *loop)
{
    if (loop)
    {
        loop->running = false;
    }
}

int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return -1;
    }

    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}