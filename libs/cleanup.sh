#!/bin/bash

# remove antlr parser generator
rm antlr-4.6-complete.jar

# remove lib directories
for folder in ./* ; do
    if [ -d "$folder" ]; then
        rm -rf $folder
    fi
done
