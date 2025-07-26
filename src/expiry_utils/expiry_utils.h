#ifndef EXPIRY_UTILS_H
#define EXPIRY_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../redis_db/redis_db.h"
#include <sys/time.h>

// Get current time in milliseconds
long long get_current_time_ms();

// Set expiry in milliseconds
void set_expiry_ms(redis_object_t *obj, int milliseconds);

// Check if expired
int is_expired(redis_object_t *obj);

#endif