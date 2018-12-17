#!/bin/bash

MG_PATH='../../../build/memgraph'
MG_CLIENT_PATH='../../../build/tools/src/mg_client'
DATA_PATH='../../../tests/qa/tck_engine/tests/movie_example/graphs/movies_graph.cypher'

# Run Memgraph
$MG_PATH &
MG_PID=$!
disown 

echo "Memgraph running with pid $MG_PID"

echo "Loading data into Memgraph"
sleep 1 # So Memgraph has time to start
$MG_CLIENT_PATH --use-ssl=false < $DATA_PATH

echo "Testing..."
python3 -m unittest integration_test.py

echo "Done."

kill $MG_PID