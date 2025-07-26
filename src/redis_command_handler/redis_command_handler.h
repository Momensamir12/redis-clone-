#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include "../resp_praser/resp_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>

char * handle_command (redis_db_t *db, char *buffer);
void handle_set_command(redis_db_t *db, const char *key, const char *value);
char* handle_get_command(redis_db_t *db, const char *key);

#endif