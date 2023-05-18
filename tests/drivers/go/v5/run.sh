#!/bin/bash -e

# check if go is installed
for i in go; do
  if ! which $i >/dev/null; then
    echo "Please install $i!"
    exit 1
  fi
done

go get github.com/neo4j/neo4j-go-driver/v5

go run docs_quick_start.go
