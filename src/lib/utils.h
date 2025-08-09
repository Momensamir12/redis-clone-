#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h> 

static inline bool isInteger(const char *str) {
    if (str == NULL || *str == '\0') { 
        return false;
    }

    int i = 0;
    if (str[0] == '-') {
        i = 1; 
    }

    if (str[i] == '\0') {
        return false; 
    }

    for (; str[i] != '\0'; i++) {
        if (!isdigit(str[i])) {
            return false; 
        }
    }

    return true; 
}


static inline get_digit_count(int num) {
    if (num == 0) return 1;
    int count = 0;
    if (num < 0) {
        count = 1; // for negative sign
        num = -num;
    }
    while (num > 0) {
        count++;
        num /= 10;
    }
    return count;
}

#endif