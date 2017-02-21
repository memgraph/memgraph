#!/bin/bash

# install all dependencies on debian based operating systems
for pkg in wget git cmake uuid-dev clang-3.8; do
    dpkg -s $pkg 2>/dev/null >/dev/null || sudo apt-get -y install $pkg
done

# setup libs (download)
cd libs
./setup.sh
cd ..

echo "DONE"
