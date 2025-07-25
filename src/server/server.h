#ifndef SERVER_H
#define SERVER_H

#include <netinet/in.h>

typedef struct {
    int fd;
    int port;
    struct sockaddr_in addr;
} server_t;

server_t* server_create(int port);
void server_destroy(server_t* server);
int server_accept_client(server_t* server, struct sockaddr_in* client_addr);

#endif