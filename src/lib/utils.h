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

#endif