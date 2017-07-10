#!/bin/bash

if [ ! -d ve3 ]; then
    virtualenv -p python3 ve3 || exit 1
    source ve3/bin/activate
    pip install -r requirements.txt || exit 1
else
    source ve3/bin/activate
fi

# TODO: apt-get install python3-tk
