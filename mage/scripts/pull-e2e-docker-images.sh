#!/bin/bash

# This is for pulling all necessary images for e2e migration tests in the background in CI
docker pull mysql:8.0
docker pull postgres:15.14
docker pull neo4j:5.10.0
