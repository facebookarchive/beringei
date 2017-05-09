#!/bin/bash

# Fail script on any error.
set -e

# Build thrift files.
for THRIFT_FILE in "$@"; do
    echo "Building file: " $THRIFT_FILE
    python2 -mthrift_compiler.main --gen cpp2 "$THRIFT_FILE" -I../..
done
