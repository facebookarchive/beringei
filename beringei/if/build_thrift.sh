#!/bin/bash

# fail script on any error
set -e

# build every thrift file
for THRIFT_FILE in $(ls *.thrift); do
    echo "Building file: " $THRIFT_FILE
    PYTHONPATH=/tmp/fbthrift-2016.11.07.00/thrift/.python-local/lib/python  python2 -mthrift_compiler.main --gen cpp2 $THRIFT_FILE -I../..
done
