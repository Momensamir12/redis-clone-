#include "redis_server.h"
#include "../server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <ctype.h>
#include "../redis_command_handler/redis_command_handler.h"


static void handle_server_accept(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data);

redis_server_t* redis_server_create(int port)
{
    redis_server_t *redis = calloc(1, sizeof(redis_server_t));
    if(!redis)
      return NULL;
    
    server_t *server = server_create(port);
    if(!server){
        free(redis);
        return NULL;
    }
    redis->server = server;
    
    event_loop_t *event_loop = event_loop_create();
    if(!event_loop){
        server_destroy(server);
        free(redis);
        return NULL;
    }
    redis->event_loop = event_loop;
    if(event_loop_add_fd(event_loop, server->fd, EPOLLIN, handle_server_accept, redis) < 0){
       server_destroy(server);
       event_loop_destroy(event_loop);
       free(redis);
       return NULL;
    }

    printf("Redis server listening on port %d\n", port);
    return redis;
}

void redis_server_destroy(redis_server_t *redis) {
    if (!redis) return;
    
    if (redis->event_loop) {
        event_loop_destroy(redis->event_loop);
    }
    
    if (redis->server) {
        server_destroy(redis->server);  // Assuming you have this
    }
    
    free(redis);
}

void redis_server_run(redis_server_t *redis) {
    if (!redis || !redis->event_loop) return;
    
    event_loop_run(redis->event_loop);
}

// Handler for accepting new connections
static void handle_server_accept(event_loop_t *event_loop, int fd, uint32_t events, void *data) {
    redis_server_t *redis = (redis_server_t *)data;
    
    struct sockaddr_in client_addr;
    int client_fd = server_accept_client(redis->server, &client_addr);
    
    if (client_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("accept");
        }
        return;
    }
    
    // Set non-blocking
    if (set_nonblocking(client_fd) < 0) {
        perror("set_nonblocking");
        close(client_fd);
        return;
    }
    
    // Add client to event loop
    if (event_loop_add_fd(event_loop, client_fd, EPOLLIN | EPOLLET, 
                          handle_client_data, redis) < 0) {
        perror("event_loop_add_fd");
        close(client_fd);
        return;
    }
    
    printf("Client connected (fd=%d)\n", client_fd);
}

static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data) {
    char buffer[1024];  
    
    if (events & EPOLLIN) {
        while (1) {  
            ssize_t bytes_read = read(fd, buffer, sizeof(buffer) - 1);
            printf("handling a client command \n");
            if (bytes_read > 0) {
                buffer[bytes_read] = '\0';
                printf("Received from client %d: %s", fd, buffer);
                char * response = handle_command(buffer);
                send(fd, response, strlen(response), MSG_NOSIGNAL);
            }
                
             else if (bytes_read == 0) {
                printf("Client %d disconnected\n", fd);
                event_loop_remove_fd(loop, fd);
                close(fd);
                break;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                } else {
                    perror("read");
                    event_loop_remove_fd(loop, fd);
                    close(fd);
                    break;
                }
            }
        }
    }
    
    if (events & (EPOLLHUP | EPOLLERR)) {
        printf("Client %d error or hangup\n", fd);
        event_loop_remove_fd(loop, fd);
        close(fd);
    }
}
