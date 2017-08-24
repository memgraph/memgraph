#!/bin/bash

# go to script directory
working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${working_dir}

# remove antlr parser generator
rm *.jar

# remove lib directories
for folder in * ; do
    if [ -d "$folder" ]; then
        rm -rf $folder
    fi
done
