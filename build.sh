#!/bin/bash

while [[ $# > 1 ]]
do
key="$1"
case $key in
    -t|--target)
    target="$2"
    shift
    ;;
esac
shift
done

if [[ -z $target ]]; then
    target="all"
fi

cd src/api && python link_resources.py && cd ../..
make clean && make $target
