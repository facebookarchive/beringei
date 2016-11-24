#!/bin/bash

DIR=$(dirname "$0")

find "$DIR" -type f -name '*.cpp' -execdir clang-format-3.9 -i {} +
find "$DIR" -type f -name '*.h' -execdir clang-format-3.9 -i {} +
