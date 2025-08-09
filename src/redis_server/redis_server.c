#include "redis_server.h"
#include "../server/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>  
#include <ctype.h>
#include "../redis_command_handler/redis_command_handler.h"
#include "../lib/list.h"
#include "../clients/client.h"
#include "../resp_praser/resp_parser.h"
#include "../lib/utils.h"
static void handle_server_accept(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_timer_interrupt(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_master_data(event_loop_t *loop, int fd, uint32_t events, void *data);
static void send_next_handshake_command(redis_server_t *server);

static void generate_replication_id(char *repl_id);
static void connect_to_master(redis_server_t *server);

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
    generate_replication_id(info->replication_id);
    
    // Store the replication info in the server structure
    server->replication_info = info;
    connect_to_master(server);

    
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

static void connect_to_master(redis_server_t *server)
{
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) return;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server->replication_info->master_port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    
    if (connect(master_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(master_fd);
        return;
    }
    
    server->replication_info->master_fd = master_fd;
    server->replication_info->handshake_step = 0; // Track current step
    
    event_loop_add_fd(server->event_loop, master_fd, EPOLLIN | EPOLLET, handle_master_data, NULL);
    
    // Start with step 1: PING
    send_next_handshake_command(server);
}

static void send_next_handshake_command(redis_server_t *server)
{
    int fd = server->replication_info->master_fd;
    
    switch (server->replication_info->handshake_step) {
        case 0: // Send PING
            {
                char *ping_cmd = "*1\r\n$4\r\nPING\r\n";
                send(fd, ping_cmd, strlen(ping_cmd), MSG_NOSIGNAL);
                printf("Sent PING\n");
            }
            break;
            
case 1: // Send REPLCONF listening-port
    {
        char *cmd = "*3\r\n$8\r\nREPLCONF\r\n$13\r\nlistening-port\r\n$4\r\n6380\r\n";
        send(fd, cmd, strlen(cmd), MSG_NOSIGNAL);
        printf("Sent hardcoded REPLCONF\n");
    }
    break;
            
        case 2: // Send REPLCONF capa psync2
            {
                char *capa_cmd = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                send(fd, capa_cmd, strlen(capa_cmd), MSG_NOSIGNAL);
                printf("Sent REPLCONF capa psync2\n");
            }
            break;
            
        case 3: // Send PSYNC
            {
                char *psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                send(fd, psync_cmd, strlen(psync_cmd), MSG_NOSIGNAL);
                printf("Sent PSYNC\n");
            }
            break;
    }
}

static void handle_master_data(event_loop_t *loop, int fd, uint32_t events, void *data)
{
    redis_server_t *server = (redis_server_t *)loop->server_data;  
    
    char buffer[1024];
    ssize_t bytes_read = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) return;
    
    buffer[bytes_read] = '\0';
    printf("Received: %s", buffer);
    
    // Check if response is positive, then move to next step
    if (strstr(buffer, "+PONG") || strstr(buffer, "+OK") || strstr(buffer, "+FULLRESYNC")) {
        server->replication_info->handshake_step++;
        
        if (server->replication_info->handshake_step < 4) {
            // Send next command
            send_next_handshake_command(server);
        } else {
            printf("Handshake complete!\n");
        }
    }
}