#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../redis_db/redis_db.h"
#include <sys/time.h>


long long get_current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Set expiry in milliseconds
void set_expiry_ms(redis_object_t *obj, int milliseconds) {
    obj->expiry = get_current_time_ms() + milliseconds;
}

// Check if expired
int is_expired(redis_object_t *obj) {
    if (obj->expiry == 0) return 0;  // No expiry
    return get_current_time_ms() > obj->expiry;
}