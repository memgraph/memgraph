#!/bin/bash

# This scrips runs clang-format recursively on all files under specified
# directories. Formatting configuration is defined in .clang-format.

clang_format="clang-format"

for directory in src tests
do
    echo "formatting code under $directory/"
    find "$directory" \( -name '*.hpp' -or -name '*.cpp' \) -print0 | xargs -0 "${clang_format}" -i
done
