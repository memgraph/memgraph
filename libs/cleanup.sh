#!/bin/bash

# go to script directory
working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${working_dir}

# remove archives
rm *.jar *.tar.gz *.tar 2>/dev/null

# remove lib directories
for folder in * ; do
    if [ -d "$folder" ]; then
        rm -rf $folder
    fi
done
