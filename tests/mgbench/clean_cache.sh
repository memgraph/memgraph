#! /bin/bash

cd .cache/datasets/pokec/medium
rm -rf dataset.cypher memgraph.cypher
touch dataset.cypher memgraph.cypher
cd ..
cd small
rm -rf dataset.cypher memgraph.cypher
touch dataset.cypher memgraph.cypher
