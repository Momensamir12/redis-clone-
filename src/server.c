#include "server.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>     
#include <sys/epoll.h>

server_t* server_create (int port)
{
    server_t* server = malloc(sizeof(server_t));
    if (!server) return NULL; 
    memset(server, 0, sizeof(server_t));
    server->port = port;
    server->fd = socket(AF_INET, SOCK_STREAM, 0);
    if(server->fd < 0){
        free(server);
        return NULL;
    }
    int reuse = 1;
    setsockopt(server->fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        server->addr.sin_family = AF_INET;
    server->addr.sin_port = htons(port);
    server->addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    // Bind and listen
    if (bind(server->fd, (struct sockaddr*)&server->addr, sizeof(server->addr)) < 0 ||
        listen(server->fd, 5) < 0) {
        close(server->fd);
        free(server);
        return NULL;
    }
    return server;
}
void server_destroy(server_t* server) {
    if (server) {
        close(server->fd);
        free(server);
    }
}

int server_accept_client(server_t* server, struct sockaddr_in* client_addr) {
    printf("accepting client connection \n");
    socklen_t len = sizeof(*client_addr);
    return accept(server->fd, (struct sockaddr*)client_addr, &len);
}

