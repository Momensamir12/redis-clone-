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
#include "hash_table/hash_table.h"
#include "redis_db/redis_db.h"
#include "rdb/io_buffer.h"
#include "rdb/rdb.h"
#include "lib/radix_tree.h"

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

void test_rdb_save_load()
{
    printf("=== Testing RDB Save/Load Functionality ===\n");

    // 1. Create a test database and populate it with data
    redis_db_t *test_db = redis_db_create(0); // Database 0

    printf("Adding test data to database...\n");

    // Add various string values
    redis_object_t *obj1 = redis_object_create_string("Hello World");
    redis_object_t *obj2 = redis_object_create_string("Sosuke");
    redis_object_t *obj3 = redis_object_create_string("Testing 123");
    redis_object_t *obj4 = redis_object_create_string("RDB Format");

    // Add some numeric values
    redis_object_t *obj5 = redis_object_create_number("42");
    redis_object_t *obj6 = redis_object_create_number("100");

    // Add list test cases
    redis_object_t *list1 = redis_object_create_list();
    redis_list_t *list_data1 = (redis_list_t *)list1->ptr;
    list_rpush(list_data1, "apple");
    list_rpush(list_data1, "banana");
    list_rpush(list_data1, "cherry");

    redis_object_t *list2 = redis_object_create_list();
    redis_list_t *list_data2 = (redis_list_t *)list2->ptr;
    list_rpush(list_data2, "first");
    list_rpush(list_data2, "second");
    list_rpush(list_data2, "third");
    list_rpush(list_data2, "fourth");

    // Empty list test case
    redis_object_t *empty_list = redis_object_create_list();

    // Large list test case
    redis_object_t *large_list = redis_object_create_list();
    redis_list_t *large_list_data = (redis_list_t *)large_list->ptr;
    for (int i = 0; i < 10; i++)
    {                            // Reduced from 100 to 10 for easier testing
        char *item = malloc(20); // Allocate memory for each item
        snprintf(item, 20, "item_%d", i);
        list_rpush(large_list_data, item);
    }

    // === NEW: Add stream test cases ===
    printf("Creating stream test cases...\n");

    // Test case 1: Simple stream with a few entries
    redis_stream_t *stream1 = redis_stream_create();
    redis_object_t *stream1_obj = redis_object_create_stream(stream1);

    // Add some entries to stream1
    const char *field_names1[] = {"name", "age"};
    const char *field_values1[] = {"Alice", "25"};
    int error_code;

    char *entry_id1 = redis_stream_add(stream1, "*", field_names1, field_values1, 2, &error_code);
    if (entry_id1)
    {
        printf("Added entry to stream1: %s\n", entry_id1);
        free(entry_id1);
    }

    const char *field_names2[] = {"name", "age", "city"};
    const char *field_values2[] = {"Bob", "30", "Tokyo"};
    char *entry_id2 = redis_stream_add(stream1, "*", field_names2, field_values2, 3, &error_code);
    if (entry_id2)
    {
        printf("Added entry to stream1: %s\n", entry_id2);
        free(entry_id2);
    }

    // Test case 2: Stream with specific IDs
    redis_stream_t *stream2 = redis_stream_create();
    redis_object_t *stream2_obj = redis_object_create_stream(stream2);

    const char *field_names3[] = {"sensor", "temperature", "humidity"};
    const char *field_values3[] = {"DHT22", "23.5", "65.2"};
    char *entry_id3 = redis_stream_add(stream2, "1640995200000-0", field_names3, field_values3, 3, &error_code);
    if (entry_id3)
    {
        printf("Added entry to stream2: %s\n", entry_id3);
        free(entry_id3);
    }

    const char *field_names4[] = {"sensor", "temperature", "humidity", "pressure"};
    const char *field_values4[] = {"BME280", "24.1", "67.8", "1013.25"};
    char *entry_id4 = redis_stream_add(stream2, "1640995260000-0", field_names4, field_values4, 4, &error_code);
    if (entry_id4)
    {
        printf("Added entry to stream2: %s\n", entry_id4);
        free(entry_id4);
    }

    // Test case 3: Empty stream
    redis_stream_t *empty_stream = redis_stream_create();
    redis_object_t *empty_stream_obj = redis_object_create_stream(empty_stream);

    // Test case 4: Stream with single field entries
    redis_stream_t *stream3 = redis_stream_create();
    redis_object_t *stream3_obj = redis_object_create_stream(stream3);

    for (int i = 0; i < 5; i++)
    {
        const char *field_names[] = {"counter"};
        char value_buf[20];
        snprintf(value_buf, sizeof(value_buf), "%d", i * 10);
        const char *field_values[] = {value_buf};

        char *entry_id = redis_stream_add(stream3, "*", field_names, field_values, 1, &error_code);
        if (entry_id)
        {
            printf("Added entry to stream3: %s (counter=%s)\n", entry_id, value_buf);
            free(entry_id);
        }
    }

    // Store ALL objects in hash table
    hash_table_set(test_db->dict, "greeting", obj1);
    hash_table_set(test_db->dict, "Aizen", obj2);
    hash_table_set(test_db->dict, "test_key", obj3);
    hash_table_set(test_db->dict, "format", obj4);
    hash_table_set(test_db->dict, "answer", obj5);
    hash_table_set(test_db->dict, "score", obj6);

    // Add the lists to the hash table
    hash_table_set(test_db->dict, "fruits", list1);
    hash_table_set(test_db->dict, "sequence", list2);
    hash_table_set(test_db->dict, "empty_list", empty_list);
    hash_table_set(test_db->dict, "items", large_list);

    // Add the streams to the hash table
    hash_table_set(test_db->dict, "users", stream1_obj);
    hash_table_set(test_db->dict, "sensors", stream2_obj);
    hash_table_set(test_db->dict, "empty_stream", empty_stream_obj);
    hash_table_set(test_db->dict, "counters", stream3_obj);

    printf("Test database populated with %zu keys\n", test_db->dict->count);

    // 2. Test the iterator by printing all keys first
    printf("\n=== Testing Hash Table Iterator ===\n");
    hash_table_iterator_t *iter = hash_table_iterator_create(test_db->dict);
    char *key;
    void *value;
    int count = 0;

    while (hash_table_iterator_next(iter, &key, &value))
    {
        redis_object_t *obj = (redis_object_t *)value;
        if (obj->type == REDIS_LIST)
        {
            redis_list_t *list = (redis_list_t *)obj->ptr;
            printf("Key: '%s' -> Type: LIST, Length: %zu\n", key, list->length);

            // Print first few elements for verification
            if (list->length > 0)
            {
                printf("  First few elements: ");
                list_node_t *node = list->head;
                int print_count = (list->length < 5) ? list->length : 5;
                for (int i = 0; i < print_count && node; i++)
                {
                    printf("'%s' ", (char *)node->data);
                    node = node->next;
                }
                if (list->length > 5)
                    printf("...");
                printf("\n");
            }
        }
        else if (obj->type == REDIS_STREAM)
        {
            redis_stream_t *stream = (redis_stream_t *)obj->ptr;
            printf("Key: '%s' -> Type: STREAM, Length: %zu, Last ID: %s\n",
                   key, stream->length, stream->last_id ? stream->last_id : "NULL");

            // Print first few entries for verification
            if (stream->length > 0)
            {
                printf("  Stream entries:\n");
                void **results = NULL;
                int entry_count = 0;

                radix_tree_range(stream->entries_tree, "-", "+", &results, &entry_count);

                int print_count = (entry_count < 3) ? entry_count : 3;
                for (int i = 0; i < print_count; i++)
                {
                    stream_entry_t *entry = (stream_entry_t *)results[i];
                    printf("    ID: %s, Fields: ", entry->id);
                    for (size_t j = 0; j < entry->field_count; j++)
                    {
                        printf("%s=%s", entry->fields[j].name, entry->fields[j].value);
                        if (j < entry->field_count - 1)
                            printf(", ");
                    }
                    printf("\n");
                }
                if (entry_count > 3)
                    printf("    ... (%d more entries)\n", entry_count - 3);

                free(results);
            }
        }
        else
        {
            printf("Key: '%s' -> Type: %d, Value: '%s'\n",
                   key, obj->type, (char *)obj->ptr);
        }
        count++;
    }

    hash_table_iterator_destroy(iter);
    printf("Iterator found %d keys\n", count);

    // 3. Save to RDB file
    printf("\n=== Testing RDB Save ===\n");
    const char *rdb_filename = "test_dump.rdb";

    // Open file for writing
    int fd = open(rdb_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1)
    {
        perror("Failed to open RDB file for writing");
        redis_db_destroy(test_db);
        return;
    }

    io_buffer rdb_buffer;
    buffer_init_with_fd(&rdb_buffer, fd);

    // Call your RDB save function
    if (rdb_save_database(&rdb_buffer, test_db) == 0)
    {
        printf("Successfully saved database to %s\n", rdb_filename);

        // Flush and close
        buffer_flush(&rdb_buffer);
        close(fd);

        // 4. Load and verify the RDB file
        printf("\n=== Testing RDB Load ===\n");
        printf("Loading RDB file: %s\n", rdb_filename);
        redis_db_t *test_load = redis_db_create(1);
        rdb_load_full(rdb_filename, test_load);
        hash_table_iterator_t *load_iter = hash_table_iterator_create(test_load->dict);
        char *loaded_key;
        void *loaded_value;
        int loaded_count = 0;

        while (hash_table_iterator_next(load_iter, &loaded_key, &loaded_value))
        {
            redis_object_t *loaded_obj = (redis_object_t *)loaded_value;
            printf("Loaded Key: '%s' -> Type: %s\n", loaded_key, redis_type_to_string(loaded_obj->type));
            loaded_count++;
        }
    }
    else
    {
        printf("Failed to save database to RDB file\n");
        close(fd);
    }

    // 6. Cleanup
    redis_db_destroy(test_db);
    printf("\n=== Test Complete ===\n");
}



int main(int argc, char *argv[])
{
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    int port = REDIS_DEFAULT_PORT;
    int8_t is_replica = 0;
    char *master_host = NULL;
    int master_port = 0;
    char *rdb_dir = "/tmp";           
    char *rdb_filename = "dump.rdb";  
    //test_rdb_save_load();


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
              else if (strcmp(argv[i], "--dir") == 0)
        {
            if (i + 1 >= argc)
            {
                fprintf(stderr, "Error: --dir requires a value\n");
                print_usage(argv[0]);
                return 1;
            }
            rdb_dir = argv[i + 1];
            i++; // Skip the next argument
        }
        else if (strcmp(argv[i], "--dbfilename") == 0)
        {
            if (i + 1 >= argc)
            {
                fprintf(stderr, "Error: --dbfilename requires a value\n");
                print_usage(argv[0]);
                return 1;
            }
            rdb_filename = argv[i + 1];
            i++; // Skip the next argument
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

    g_server->rdb_filename = rdb_filename;
    g_server->rdb_dir = rdb_dir;
    redis_server_run(g_server);

    // Cleanup
    redis_server_destroy(g_server);
    // Initialize monitoring if requested

    return 0;
}
