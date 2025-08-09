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
#include "../lib/list.h"
#include "../clients/client.h"

static void handle_server_accept(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_timer_interrupt(event_loop_t *loop, int fd, uint32_t events, void *data);
static void generate_replication_id(char *repl_id);

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
    redis->db = redis_db_create(0);
    init_command_table();
    
    // Initialize client lists
    redis->clients = list_create();
    redis->blocked_clients = list_create();
    if (!redis->clients || !redis->blocked_clients) {
        server_destroy(server);
        redis_db_destroy(redis->db);
        free(redis);
        return NULL;
    }
    
    event_loop_t *event_loop = event_loop_create();
    if(!event_loop){
        server_destroy(server);
        redis_db_destroy(redis->db);
        list_destroy(redis->clients);
        list_destroy(redis->blocked_clients);
        free(redis);
        return NULL;
    }
    redis->event_loop = event_loop;
    
    
    if(event_loop_add_fd(event_loop, server->fd, EPOLLIN, handle_server_accept, redis) < 0){
       server_destroy(server);
       event_loop_destroy(event_loop);
       redis_db_destroy(redis->db);
       list_destroy(redis->clients);
       list_destroy(redis->blocked_clients);
       free(redis);
       return NULL;
    }
    
    if(event_loop_add_fd(event_loop, event_loop->timer_fd, EPOLLIN, handle_timer_interrupt, redis) < 0){
       server_destroy(server);
       event_loop_destroy(event_loop);
       redis_db_destroy(redis->db);
       list_destroy(redis->clients);
       list_destroy(redis->blocked_clients);
       free(redis);
       return NULL;
    }
    event_loop->server_data = redis;

    printf("Redis server listening on port %d\n", port);
    return redis;
}

void redis_server_destroy(redis_server_t *redis) {
    if (!redis) return;
    
    // Clean up all clients
    if (redis->clients) {
        list_node_t *node = redis->clients->head;
        while (node) {
            client_t *client = (client_t *)node->data;
            if (redis->event_loop) {
                event_loop_remove_fd(redis->event_loop, client->fd);
            }
            close(client->fd);
            free_client(client);
            node = node->next;
        }
        list_destroy(redis->clients);
    }
    
    // blocked_clients list doesn't own the clients, just references
    if (redis->blocked_clients) {
        list_destroy(redis->blocked_clients);
    }
    
    if (redis->event_loop) {
        event_loop_destroy(redis->event_loop);
    }
    
    if (redis->server) {
        server_destroy(redis->server);
    }
    
    if(redis->db) {
        redis_db_destroy(redis->db);
    }
    if(redis->replication_info)
    {
      free(redis->replication_info->master_host);
      free(redis->replication_info);
    }  

    free(redis);
}

void redis_server_run(redis_server_t *redis) {
    if (!redis || !redis->event_loop) return;
    
    event_loop_run(redis->event_loop);
}

// redis_server.c

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
    
    // Create client object
    client_t *client = create_client(client_fd);
    if (!client) {
        perror("create_client");
        close(client_fd);
        return;
    }
    
    // Add to server's client list (not the global one)
    add_client_to_list(redis->clients, client);
    
    // Add client to event loop - pass client as data
    if (event_loop_add_fd(event_loop, client_fd, EPOLLIN | EPOLLET, 
                          handle_client_data, client) < 0) {
        perror("event_loop_add_fd");
        remove_client_from_list(redis->clients, client);
        free_client(client);
        close(client_fd);
        return;
    }
    
    printf("Client connected (fd=%d), total clients: %zu\n", 
           client_fd, list_length(redis->clients));
}

// redis_server.c - Update handle_client_data
static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data) {
    char buffer[1024];
    client_t *client = (client_t *)data;
    
    // Need to get redis_server - you could store it in event_loop or pass differently
    // For now, let's store it in the event_loop structure
    redis_server_t *redis = (redis_server_t *)loop->server_data;  
    
    if (events & EPOLLIN) {
        // Skip if client is blocked
        if (client->is_blocked) {
            return;
        }
        
        while (1) {  
            ssize_t bytes_read = read(fd, buffer, sizeof(buffer) - 1);
            
            if (bytes_read > 0) {
                buffer[bytes_read] = '\0';
                printf("Received from client %d: %s", fd, buffer);
                
                // Pass server and client to command handler
                char *response = handle_command(redis, buffer, client);
                
                // NULL response means client was blocked
                if (response) {
                    send(fd, response, strlen(response), MSG_NOSIGNAL);
                    free(response);
                }
            }
            else if (bytes_read == 0) {
                // Client disconnected
                printf("Client %d disconnected\n", fd);
                
                // Clean up
                if (client->is_blocked) {
                    remove_client_from_list(redis->blocked_clients, client);
                }
                remove_client_from_list(redis->clients, client);
                event_loop_remove_fd(loop, fd);
                close(fd);
                free_client(client);
                break;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                } else {
                    perror("read");
                    // Error - disconnect
                    if (client->is_blocked) {
                        remove_client_from_list(redis->blocked_clients, client);
                    }
                    remove_client_from_list(redis->clients, client);
                    event_loop_remove_fd(loop, fd);
                    close(fd);
                    free_client(client);
                    break;
                }
            }
        }
    }
    
    if (events & (EPOLLHUP | EPOLLERR)) {
        printf("Client %d error or hangup\n", fd);
        if (client->is_blocked) {
            remove_client_from_list(redis->blocked_clients, client);
        }
        remove_client_from_list(redis->clients, client);
        event_loop_remove_fd(loop, fd);
        close(fd);
        free_client(client);
    }
}

// Update timer interrupt handler
static void handle_timer_interrupt(event_loop_t *loop, int fd, uint32_t events, void *data) {
    (void)loop;
    (void)events;
    redis_server_t *redis = (redis_server_t *)data;
    
    // Clear timer
    uint64_t expirations;
    read(fd, &expirations, sizeof(expirations));
    
    // Check blocked clients for timeout
    check_blocked_clients_timeout(redis);
}

int redis_server_configure_master(redis_server_t *server)
{
    if (!server) {
        return -1;
    }
    
    // Allocate memory for the struct itself, not a pointer to it
    replication_info_t *info = malloc(sizeof(replication_info_t));
    if (!info) {
        return -1;
    }
    
    info->role = MASTER;
    info->connected_slaves = 0;
    info->master_host = NULL;  // Masters don't have a master
    info->master_port = 0;
    generate_replication_id(info->replication_id);
    info->master_repl_offset = 0;
    // Store the replication info in the server structure
    server->replication_info = info;
    
    return 0;
}

int redis_server_configure_replica(redis_server_t *server, char* master_host, int master_port)
{
    if (!server || !master_host) {
        return -1;
    }
    
    // Allocate memory for the struct itself, not a pointer to it
    replication_info_t *info = malloc(sizeof(replication_info_t));
    if (!info) {
        return -1;
    }
    
    info->role = SLAVE;
    info->connected_slaves = 0;  // Slaves don't have slaves, so 0 not -1
    
    // Store master connection information
    info->master_host = strdup(master_host);  // Make a copy of the host string
    if (!info->master_host) {
        free(info);
        return -1;
    }
    info->master_port = master_port;
    
    // Store the replication info in the server structure
    server->replication_info = info;
    
    return 0;
}

static void generate_replication_id(char *repl_id) {
    const char hex_chars[] = "0123456789abcdef";
    
    srand(time(NULL));
    
    for (int i = 0; i < 40; i++) {
        repl_id[i] = hex_chars[rand() % 16];
    }
    repl_id[40] = '\0';
}