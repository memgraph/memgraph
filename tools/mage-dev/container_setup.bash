#!/bin/bash

echo "Running container setup..."
# NOTE: Feel free to tweak the below code to what's required for you tests.

#rm -f /usr/lib/memgraph/query_modules/embed_worker/embed_worker.py
mkdir -p /usr/lib/memgraph/query_modules/embed_worker
ln -s /app/embed_worker.py /usr/lib/memgraph/query_modules/embed_worker/embed_worker.py

#rm -f /usr/lib/memgraph/query_modules/embeddings.py
ln -s /app/embeddings.py /usr/lib/memgraph/query_modules/embeddings.py
