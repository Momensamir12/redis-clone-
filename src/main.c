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


#define BUFFER_SIZE 1024
#define MAX_EVENTS 10
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
	int epollfd;
	int server_fd = server->fd;
	epollfd = epoll_create1(0);
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = server_fd;
	epoll_ctl(epollfd, EPOLL_CTL_ADD, server_fd, &ev);
	struct sockaddr_in client_addr;
    while (1) { 
    struct epoll_event events[MAX_EVENTS];		
    int nfds = epoll_wait(epollfd,&events, MAX_EVENTS, -1);

       for (int i = 0; i < nfds; i++) {
        if (events[i].data.fd == server_fd) {
            // New connection!
			struct sockaddr_in addr;
			int client_fd = server_accept_client(server, &addr);
			if(client_fd >= 0){
		    int flags = fcntl(client_fd, F_GETFL, 0);
            fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLET;  // Edge-triggered
            ev.data.fd = client_fd;
            epoll_ctl(epollfd, EPOLL_CTL_ADD, client_fd, &ev);
			}
		}
		else
		{
			char *command_buffer = malloc(20 * sizeof(char));
			int client_fd = events[i].data.fd;
			ssize_t bytes_read = read(client_fd, command_buffer, 20);
			if (bytes_read <= 1)
			{
				printf("Read failed: %s \n", strerror(errno));
				free(command_buffer);
				close(server_fd);
				return 1;
			}
			printf("Received command: %s\n", command_buffer);
			free(command_buffer);

			char *PONG = "+PONG\r\n";
			send(client_fd, PONG, strlen(PONG), 0);
		}
	}
}

	return 0;
}
