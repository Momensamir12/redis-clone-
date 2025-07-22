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


#define BUFFER_SIZE 1024
int main() {
	// Disable output buffering
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	server_t *server = server_create(6379);
	  if (!server) {
        fprintf(stderr, "Failed to create server\n");
        return 1;
    }
	printf("Redis listening in port 6379\n");
    while (1) {  // Outer loop for multiple clients
    
    printf("Client connected\n");
	struct sockaddr_in client_addr;

    while (1) {  // Inner loop for multiple requests from current client
	    int connection_fd = server_accept_client(server, &client_addr);
        char *command_buffer = malloc(20 * sizeof(char));
        ssize_t bytes_read = read(connection_fd, command_buffer, 20);

        if (bytes_read <= 0) {  // Changed from <= 1 to <= 0
            printf("Client disconnected\n");
            free(command_buffer);
            close(connection_fd);
            break;  // Break inner loop, go back to accept new client
        }
        
        printf("Received command: %s\n", command_buffer);
        free(command_buffer);

        char* PONG = "+PONG\r\n";
        send(connection_fd, PONG, strlen(PONG), 0);
    }
}

	return 0;
}
