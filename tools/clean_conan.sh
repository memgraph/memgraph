#!/bin/bash
set -euo pipefail

# check if env exists
if [ ! -f "env/bin/activate" ]; then
    echo "Warning: Virtual environment not found at env/bin/activate"
    echo "Skipping conan cache cleanup"
    exit 0  
fi
source env/bin/activate
trap 'deactivate 2>/dev/null' EXIT ERR

# check if conan is installed
if ! command -v conan &> /dev/null; then
    echo "Warning: Conan not found in PATH"
    echo "Skipping conan cache cleanup"
    exit 0
fi

# remove all conan packages older than 1 week
conan remove "*#*" --lru=1w -c
