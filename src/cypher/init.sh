#!/bin/bash
echo "Cypher init..."
git submodule update --init lexertl
cd lemon
make
cd ../
make
echo "Cypher was initialized."

