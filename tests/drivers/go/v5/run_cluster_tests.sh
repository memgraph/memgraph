#!/bin/bash -e

GO_VERSION="1.18.9"
GO_VERSION_DIR="/opt/go$GO_VERSION"
if [ -f "$GO_VERSION_DIR/go/bin/go" ]; then
    export GOROOT="$GO_VERSION_DIR/go"
    export GOPATH="$HOME/go$GO_VERSION"
    export PATH="$GO_VERSION_DIR/go/bin:$PATH"
fi

# check if go is installed
for i in go; do
  if ! which $i >/dev/null; then
    echo "Please install $i!"
    exit 1
  fi
done

go get github.com/neo4j/neo4j-go-driver/v5
go run write_routing.go
go run read_routing.go
