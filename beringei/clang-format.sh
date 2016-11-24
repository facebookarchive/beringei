#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

find "$DIR" -type f -name '*.cpp' -execdir clang-format-3.9 -i {} +
find "$DIR" -type f -name '*.h' -execdir clang-format-3.9 -i {} +
