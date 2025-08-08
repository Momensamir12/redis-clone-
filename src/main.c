#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include "redis_server/redis_server.h"

#define BUFFER_SIZE 1024
#define REDIS_DEFAULT_PORT 6379

static redis_server_t *g_server = NULL;

void print_usage(const char *program_name)
{
    fprintf(stderr, "Usage: %s [--port PORT]\n", program_name);
    fprintf(stderr, "  --port PORT    Port number to listen on (default: %d)\n", REDIS_DEFAULT_PORT);
}

int parse_port(const char *port_str)
{
    char *endptr;
    long port = strtol(port_str, &endptr, 10);

    // Check for conversion errors
    if (*endptr != '\0' || port <= 0 || port > 65535)
    {
        return -1;
    }

    return (int)port;
}

int main(int argc, char *argv[])
{
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    int port = REDIS_DEFAULT_PORT;
    int8_t is_replica = 0;
    char *master_host = NULL;
    int master_port = 0;

    // Parse command line arguments
    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--port") == 0)
        {
            if (i + 1 >= argc)
            {
                fprintf(stderr, "Error: --port requires a value\n");
                print_usage(argv[0]);
                return 1;
            }

            port = parse_port(argv[i + 1]);
            if (port == -1)
            {
                fprintf(stderr, "Error: Invalid port number '%s'\n", argv[i + 1]);
                fprintf(stderr, "Port must be a number between 1 and 65535\n");
                return 1;
            }

            i++; // Skip the next argument since we consumed it as the port value
        }
        else if (strcmp(argv[i], "--replicaof") == 0)
        {
            if (i + 1 >= argc)
            {
                fprintf(stderr, "Error: --replicaof requires host and port arguments\n");
                print_usage(argv[0]);
                return 1;
            }

            // Check if it's a single argument "host port" or two separate arguments
            char *replicaof_arg = argv[i + 1];
            char *space_pos = strchr(replicaof_arg, ' ');

            if (space_pos != NULL)
            {
                // Single argument format: "localhost 6379"
                *space_pos = '\0'; // Split the string at the space
                master_host = replicaof_arg;
                master_port = parse_port(space_pos + 1);

                if (master_port == -1)
                {
                    fprintf(stderr, "Error: Invalid master port number '%s'\n", space_pos + 1);
                    fprintf(stderr, "Port must be a number between 1 and 65535\n");
                    return 1;
                }

                i++; // Skip one argument
            }
            else
            {
                // Two separate arguments format: localhost 6379
                if (i + 2 >= argc)
                {
                    fprintf(stderr, "Error: --replicaof requires host and port arguments\n");
                    print_usage(argv[0]);
                    return 1;
                }

                master_host = argv[i + 1];
                master_port = parse_port(argv[i + 2]);

                if (master_port == -1)
                {
                    fprintf(stderr, "Error: Invalid master port number '%s'\n", argv[i + 2]);
                    fprintf(stderr, "Port must be a number between 1 and 65535\n");
                    return 1;
                }

                i += 2; // Skip two arguments
            }

            is_replica = 1;
        }
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
        {
            print_usage(argv[0]);
            return 0;
        }
        else
        {
            fprintf(stderr, "Error: Unknown argument '%s'\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    if (is_replica)
    {
        printf("Starting Redis replica server on port %d, master at %s:%d\n",
               port, master_host, master_port);
    }
    else
    {
        printf("Starting Redis server on port %d\n", port);
    }

    g_server = redis_server_create(port);
    if (!g_server)
    {
        fprintf(stderr, "Failed to create Redis server on port %d\n", port);
        return 1;
    }

    if (is_replica == 0)
    {
        if (redis_server_configure_master(g_server) != 0)
        {
            fprintf(stderr, "Failed to configure server as replica of %s:%d\n",
                    master_host, master_port);
            redis_server_destroy(g_server);
            return 1;
        }
    }
    else if (is_replica == 1)
    {
        if (redis_server_configure_replica(g_server, master_host, master_port) != 0)
        {
            fprintf(stderr, "Failed to configure server as replica of %s:%d\n",
                    master_host, master_port);
            redis_server_destroy(g_server);
            return 1;
        }
    }

    // Run the event loop (blocks until stopped)
    redis_server_run(g_server);

    // Cleanup
    redis_server_destroy(g_server);

    return 0;
}