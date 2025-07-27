cmake_minimum_required(VERSION 3.10)
project(CodecraftersRedisC C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Add include directories for your headers
include_directories(
    src/event_loop
    src/hash_table
    src/redis_command_handler
    src/redis_db
    src/redis_server
    src/resp_praser
    src/server
)

# Add the executable and all source files explicitly
add_executable(redis
    src/main.c
    src/event_loop/event_loop.c
    src/hash_table/hash_table.c
    src/redis_command_handler/redis_command_handler.c
    src/redis_db/redis_db.c
    src/redis_server/redis_server.c
    src/resp_praser/resp_parser.c
    src/server/server.c
    src/expiry_utils/expiry_utils.c
    src/lib/list.c
    src/clients/client.c
)
