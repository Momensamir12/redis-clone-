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

    redis_db_destroy(test_db);
    printf("\n=== Test Complete ===\n");
}
