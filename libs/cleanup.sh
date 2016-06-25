#!/bin/bash

for folder in ./* ; do
    if [ -d "$folder" ]; then
        rm -rf $folder
    fi
done
