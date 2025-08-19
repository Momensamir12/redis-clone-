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
#include "../rdb/io_buffer.h"
#include "../rdb/rdb.h"

static void handle_server_accept(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_timer_interrupt(event_loop_t *loop, int fd, uint32_t events, void *data);
static void handle_master_data(event_loop_t *loop, int fd, uint32_t events, void *data);
static void send_next_handshake_command(redis_server_t *server);

static void generate_replication_id(char *repl_id);
static void connect_to_master(redis_server_t *server);
static void propagate_to_replicas(redis_server_t *server, const char *command_buffer, size_t buffer_len);
static int is_write_command(const char *buffer);
static void process_multiple_commands(redis_server_t *server, char *buffer, size_t buffer_len);
static int load_rdb_file(redis_server_t *server, const char *rdb_path);
static void handle_rdb_data(redis_server_t *server, const char *data, ssize_t data_len);
static void prepare_rdb_reception(redis_server_t *server);
void track_replica_bytes(redis_server_t *server, const char *command_buffer);
static void handle_master_replconf_getack(redis_server_t *server, int master_fd, const char *buffer, size_t bytes_read);
static void handle_rdb_skip(redis_server_t *server, const char *data, ssize_t data_len);

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

static void handle_client_data(event_loop_t *loop, int fd, uint32_t events, void *data) {
    char buffer[1024];
    client_t *client = (client_t *)data;
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
                
                // Check if this is a write command and we're a master
                int is_write_cmd = is_write_command(buffer);
                
                // Pass server and client to command handler
                char *response = handle_command(redis, buffer, client);
                
                // NULL response means client was blocked
                if (response) {
                    send(fd, response, strlen(response), MSG_NOSIGNAL);
                    free(response);
                    
                    // PROPAGATE TO REPLICAS IF:
                    // 1. We're a master
                    // 2. This was a write command
                    // 3. Command executed successfully (got a response)
                    if (is_write_cmd && 
                        redis->replication_info && 
                        redis->replication_info->role == MASTER &&
                        redis->replication_info->connected_slaves > 0) {
                        
                        propagate_to_replicas(redis, buffer, bytes_read);
                    }
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

static void propagate_to_replicas(redis_server_t *server, const char *command_buffer, size_t buffer_len) {
    replication_info_t *repl_info = server->replication_info;
    
    for (int i = 0; i < MAX_REPLICAS; i++) {
        if (repl_info->replicas_fd[i] != -1) {  // Check for valid fd
            ssize_t bytes_sent = send(repl_info->replicas_fd[i], command_buffer, buffer_len, MSG_NOSIGNAL);
            
            if (bytes_sent < 0) {
                printf("Failed to propagate to replica fd %zu: %s\n", 
                       repl_info->replicas_fd[i], strerror(errno));
                // Consider marking this replica as disconnected
                repl_info->replicas_fd[i] = -1;
                repl_info->connected_slaves--;
            } else {
                printf("Propagated %zu bytes to replica fd %zu\n", buffer_len, repl_info->replicas_fd[i]);
            }
        }
    }
    
    repl_info->master_repl_offset += buffer_len;
    printf("Master offset updated to: %lu\n", repl_info->master_repl_offset);
}

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
    for (int i = 0; i < MAX_REPLICAS; i++) {
        info->replicas_fd[i] = -1; 
        info->replica_ack_offsets[i] = 0;  
 
    }
    return 0;
}

int redis_server_configure_replica(redis_server_t *server, char* master_host, int master_port)
{
    if (!server || !master_host) {
        return -1;
    }
    
    replication_info_t *info = malloc(sizeof(replication_info_t));
    if (!info) {
        return -1;
    }
    
    info->role = SLAVE;
    info->connected_slaves = 0;
    
    info->master_host = strdup(master_host);
    if (!info->master_host) {
        free(info);
        return -1;
    }
    info->master_port = master_port;
    generate_replication_id(info->replication_id);
    
    info->replica_offset = 0;           // Bytes this replica has processed
    info->master_repl_offset = 0;
    
    // Initialize RDB-related fields
    info->receiving_rdb = 0;
    info->expected_rdb_size = 0;
    info->received_rdb_size = 0;
    info->rdb_fd = -1;
    
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
    if (master_fd < 0)
        return;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server->replication_info->master_port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(master_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
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

    switch (server->replication_info->handshake_step)
    {
    case 0: // Send PING
    {
        char *ping_cmd = "*1\r\n$4\r\nPING\r\n";
        send(fd, ping_cmd, strlen(ping_cmd), MSG_NOSIGNAL);
        printf("Sent PING\n");
    }
    break;

    case 1: // Send REPLCONF listening-port
    {
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", server->server->port);

        char port_cmd[200];
        snprintf(port_cmd, sizeof(port_cmd),
                 "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%zu\r\n%s\r\n",
                 strlen(port_str), port_str);

        send(fd, port_cmd, strlen(port_cmd), MSG_NOSIGNAL);
        printf("Sent REPLCONF listening-port %s\n", port_str);
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
    char buffer[4096];  // Increased buffer size
    ssize_t bytes_read = recv(fd, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes_read <= 0)
        return;

    buffer[bytes_read] = '\0';
    printf("Received: %s", buffer);

    // Handle handshake responses
    if (strstr(buffer, "+PONG") || strstr(buffer, "+OK")) {
        server->replication_info->handshake_step++;

        if (server->replication_info->handshake_step < 4) {
            send_next_handshake_command(server);
        }
    }
    else if (strstr(buffer, "+FULLRESYNC")) {
        server->replication_info->handshake_step++;
        printf("Handshake complete! Ready for replication commands.\n");
    }
    else if (server->replication_info->handshake_step >= 4) {
        // Handle RDB and subsequent commands
        if (buffer[0] == '$' && !server->replication_info->receiving_rdb) {
            // Parse RDB size and handle RDB data that might be in the same buffer
            server->replication_info->expected_rdb_size = atol(buffer + 1);
            printf("Skipping RDB file of %ld bytes\n", server->replication_info->expected_rdb_size);
            server->replication_info->receiving_rdb = 1;
            server->replication_info->received_rdb_size = 0;
            
            // Find where RDB data starts
            char *rdb_start = strstr(buffer, "\r\n");
            if (rdb_start) {
                rdb_start += 2; // Skip \r\n
                ssize_t rdb_bytes_in_buffer = bytes_read - (rdb_start - buffer);
                
                if (rdb_bytes_in_buffer > 0) {
                    handle_rdb_skip(server, rdb_start, rdb_bytes_in_buffer);
                }
            }
            return;
        }
        
        // If we're currently skipping RDB data
        if (server->replication_info->receiving_rdb) {
            handle_rdb_skip(server, buffer, bytes_read);
            return;
        }
        
        // Handle regular replication commands after RDB is complete
        if (strstr(buffer, "REPLCONF") && strstr(buffer, "GETACK")) {
            printf("Received REPLCONF GETACK command\n");
            handle_master_replconf_getack(server, fd, buffer, bytes_read); 
        }
        else {     
            track_replica_bytes(server, buffer);
            process_multiple_commands(server, buffer, bytes_read);
            printf("Command executed on replica\n");
        }
    }
}

static void handle_rdb_skip(redis_server_t *server, const char *data, ssize_t data_len)
{
    replication_info_t *repl = server->replication_info;
    
    // Calculate how much RDB data we still need to skip
    ssize_t remaining_rdb = repl->expected_rdb_size - repl->received_rdb_size;
    ssize_t rdb_bytes_in_this_buffer = (data_len < remaining_rdb) ? data_len : remaining_rdb;
    
    repl->received_rdb_size += rdb_bytes_in_this_buffer;
    
    printf("Skipping RDB data: %ld / %ld bytes (%.1f%%)\n", 
           repl->received_rdb_size, repl->expected_rdb_size,
           (double)repl->received_rdb_size / repl->expected_rdb_size * 100);
    
    // Check if RDB skipping is complete
    if (repl->received_rdb_size >= repl->expected_rdb_size) {
        printf("RDB skipping complete! Ready for commands.\n");
        repl->receiving_rdb = 0;
        
        // Check if there are commands after the RDB data in the same buffer
        if (data_len > rdb_bytes_in_this_buffer) {
            const char *commands_start = data + rdb_bytes_in_this_buffer;
            ssize_t commands_len = data_len - rdb_bytes_in_this_buffer;
            
            printf("Processing commands after RDB: %.*s", (int)commands_len, commands_start);
            
            // Process any commands that came after the RDB data
            if (strstr(commands_start, "REPLCONF") && strstr(commands_start, "GETACK")) {
                printf("Found REPLCONF GETACK after RDB data\n");
                handle_master_replconf_getack(server, server->replication_info->master_fd, 
                                             commands_start, commands_len);
            } else {
                track_replica_bytes(server, commands_start);
                // Handle other commands if needed
            }
        }
    }
}

static void prepare_rdb_reception(redis_server_t *server)
{
    // Create temp file for RDB
    snprintf(server->replication_info->rdb_temp_path, 
             sizeof(server->replication_info->rdb_temp_path),
             "/tmp/replica_rdb_%d.tmp", getpid());
    
    server->replication_info->rdb_fd = open(server->replication_info->rdb_temp_path, 
                                           O_WRONLY | O_CREAT | O_TRUNC, 0644);
    
    if (server->replication_info->rdb_fd == -1) {
        perror("Failed to create RDB temp file");
        return;
    }
    
    server->replication_info->received_rdb_size = 0;
    server->replication_info->receiving_rdb = 0; 
}

static void handle_rdb_data(redis_server_t *server, const char *data, ssize_t data_len)
{
    replication_info_t *repl = server->replication_info;
    
    // Write RDB data directly to file
    ssize_t written = write(repl->rdb_fd, data, data_len);
    if (written != data_len) {
        perror("Failed to write RDB data");
        return;
    }
    
    repl->received_rdb_size += written;
    
    printf("Received RDB data: %ld / %ld bytes (%.1f%%)\n", 
           repl->received_rdb_size, repl->expected_rdb_size,
           (double)repl->received_rdb_size / repl->expected_rdb_size * 100);
    
    // Check if RDB reception is complete
    if (repl->received_rdb_size >= repl->expected_rdb_size) {
        printf("RDB reception complete! Loading database...\n");
        
        close(repl->rdb_fd);
        repl->rdb_fd = -1;
        
        // Load the RDB file using your existing function
        if (load_rdb_file(server, repl->rdb_temp_path) == 0) {
            printf("RDB loaded successfully! Replication active.\n");
        } else {
            printf("Failed to load RDB file\n");
        }
        
        // Clean up
        unlink(repl->rdb_temp_path);
        repl->receiving_rdb = 0;
    }
}

static int load_rdb_file(redis_server_t *server, const char *rdb_path)
{
    int fd = open(rdb_path, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open RDB file for loading");
        return -1;
    }
    
    io_buffer buffer;
    buffer_init_with_fd(&buffer, fd);
    
    // Use your existing RDB load function
    int result = rdb_load_full(rdb_path, server->db);
    
    close(fd);
    
    if (result != 0) {
        fprintf(stderr, "Failed to load RDB database\n");
        return -1;
    }
    
    return 0;
}

static int is_write_command(const char *buffer) {
    // Quick RESP parsing to get the first argument (command)
    if (buffer[0] != '*') {
        return 0;  // Not a RESP array
    }
    
    // Find first command after *n\r\n$len\r\n
    const char *cmd_start = strstr(buffer, "\r\n$");
    if (!cmd_start) return 0;
    
    cmd_start = strstr(cmd_start + 3, "\r\n");
    if (!cmd_start) return 0;
    
    cmd_start += 2;  // Skip \r\n
    
    if (strncasecmp(cmd_start, "SET", 3) == 0 ||
        strncasecmp(cmd_start, "DEL", 3) == 0 ||
        strncasecmp(cmd_start, "HSET", 4) == 0 ||
        strncasecmp(cmd_start, "LPUSH", 5) == 0) {
        return 1;
    }
    
    // Skip replication commands
    if (strncasecmp(cmd_start, "REPLCONF", 8) == 0 ||
        strncasecmp(cmd_start, "PSYNC", 5) == 0 ||
        strncasecmp(cmd_start, "PING", 4) == 0) {
        return 0;
    }
    
    return 0;
}

static void process_multiple_commands(redis_server_t *server, char *buffer, size_t buffer_len) {
    char *current = buffer;
    char *buffer_end = buffer + buffer_len;
    
    while (current < buffer_end) {
        char *next_command = strstr(current + 1, "*");
        
        size_t cmd_len;
        if (next_command && next_command < buffer_end) {
            cmd_len = next_command - current;
        } else {
            cmd_len = buffer_end - current;
        }
        
        if (cmd_len > 0) {
            char *single_cmd = malloc(cmd_len + 1);
            memcpy(single_cmd, current, cmd_len);
            single_cmd[cmd_len] = '\0';
            
            printf("Executing: %s", single_cmd);
            
            // Execute command (don't track bytes here, already tracked in handle_master_data)
            char *response = handle_command(server, single_cmd, NULL);
            if (response) free(response);
            free(single_cmd);
        }
        
        if (!next_command || next_command >= buffer_end) {
            break;
        }
        current = next_command;
    }
}

void track_replica_bytes(redis_server_t *server, const char *command_buffer) {
    if (!server || !server->replication_info || !command_buffer) {
        return;
    }
    
    if (server->replication_info->role == SLAVE) {
        // Use strlen for now, but in a real implementation you'd want to track actual bytes
        size_t bytes = strlen(command_buffer);
        server->replication_info->replica_offset += bytes;
        printf("Replica offset updated: +%zu = %lu total bytes\n", 
               bytes, server->replication_info->replica_offset);
    }
}

static void handle_master_replconf_getack(redis_server_t *server, int master_fd, const char *buffer, size_t bytes_read)
{
    printf("Processing REPLCONF GETACK command\n");
    
    // Track the bytes for this GETACK command
    track_replica_bytes(server, buffer);
    
    char offset_str[32];
    snprintf(offset_str, sizeof(offset_str), "%lu", server->replication_info->replica_offset);
    
    // Count digits in offset
    int offset_digits = snprintf(NULL, 0, "%lu", server->replication_info->replica_offset);
    
    char response[256];
    snprintf(response, sizeof(response), 
            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n",
            offset_digits, offset_str);
    
    printf("Sending ACK response: %s", response);
    printf("ACK with offset: %lu\n", server->replication_info->replica_offset);
    
    // Send response back to master
    ssize_t sent = send(master_fd, response, strlen(response), MSG_NOSIGNAL);
    if (sent < 0) {
        perror("Failed to send ACK");
    } else {
        printf("Successfully sent %zd bytes ACK response\n", sent);
    }
}