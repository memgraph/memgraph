#!/bin/bash

# This workaround is necessary to run in the same virtualenv as the e2e runner.py

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
python3 "$DIR/$1"
