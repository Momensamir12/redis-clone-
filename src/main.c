#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>     
#include <sys/epoll.h>
#include "server.h"
#include <fcntl.h>      
#include <unistd.h>     
#include "redis_server.h"

#define BUFFER_SIZE 1024
#define MAX_EVENTS 10
#define REDIS_DEFAULT_PORT 6379

static redis_server_t *g_server = NULL;

int main() {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    g_server = redis_server_create(REDIS_DEFAULT_PORT);
    if (!g_server) {
        fprintf(stderr, "Failed to create Redis server\n");
        return 1;
    }
    
    // Run the event loop (blocks until stopped)
    redis_server_run(g_server);
    
    // Cleanup
    redis_server_destroy(g_server);
    
    return 0;
}
