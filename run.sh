#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface

set -e

# Create build directory if it doesn't exist
mkdir -p build
cd build

# Configure and build with CMake
cmake ..
cmake --build .

# Run the executable with --port 6379 and any additional arguments
exec ./redis --port 6379 "$@"
