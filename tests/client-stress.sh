#!/bin/bash

NUM_PARALLEL=4

if [[ "$1" != "" ]]; then
    NUM_PARALLEL=$1
fi

for i in $( seq 1 $NUM_PARALLEL ); do
    echo "CREATE (n {name: 29383}) RETURN n;" | neo4j-client --insecure --username= --password= neo4j://localhost:7687 >/dev/null 2>/dev/null &
done

running="yes"
count=0

while [[ "$running" != "" ]]; do
    running=$( pidof neo4j-client )
    num=$( echo "$running" | wc -w )
    echo "Running clients: $num"
    count=$(( count + 1 ))
    if [[ $count -gt 5 ]]; then break; fi
    sleep 1
done

if [[ "$running" != "" ]]; then
    echo "Something went wrong!"
    echo "Running PIDs: $running"
    echo "Killing leftover clients..."
    kill -9 $running >/dev/null 2>/dev/null
    wait $running 2>/dev/null
else
    echo "All ok!"
fi
