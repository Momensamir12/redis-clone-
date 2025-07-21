#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface

#!/bin/sh
set -e

gcc -o redis src/main.c  # Or change to match your file
exec ./redis "$@"

