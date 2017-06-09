#!/bin/bash

NODE=nodejs
NPM=npm

if ! which $NODE >/dev/null; then
    echo "Please install Node.JS!"
    exit 1
fi

if ! which $NPM >/dev/null; then
    echo "Please install NPM!"
    exit 1
fi

$NPM install neo4j-driver || exit 1
$NODE test.js || exit 1
