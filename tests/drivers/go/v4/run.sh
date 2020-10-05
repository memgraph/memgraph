#!/bin/bash -e

# check if go is installed

go get github.com/neo4j/neo4j-go-driver/neo4j

go run basic.go
go run transactions.go
