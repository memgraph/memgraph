#!/bin/bash -e

# check if go is installed
for i in go; do
  if ! which $i >/dev/null; then
    echo "Please install $i!"
    exit 1
  fi
done

# setup go 1.18.2
go get golang.org/dl/go1.18.2
go install golang.org/dl/go1.18.2
if [ -z ${GOPATH+x} ]; then
  export GO=$HOME/go/bin/go1.18.2
else
  export GO=$GOPATH/bin/go1.18.2
fi
$GO download

# run tests using go 1.18.2
$GO get github.com/neo4j/neo4j-go-driver/v5
$GO run docs_quick_start.go
