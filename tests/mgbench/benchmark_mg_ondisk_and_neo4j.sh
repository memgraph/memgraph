#!/bin/bash

# Help function
function show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -n, --neo4j-path     Path to Neo4j binary"
    echo "  -m, --memgraph-path  Path to Memgraph binary"
    echo "  -w, --num-workers    Number of workers for benchmark and import"
    echo "  -d, --dataset        Dataset size (small, medium, large)"
    echo "  -h, --help           Show this help message"
    exit 0
}

# Default values
neo4j_path="/usr/share/neo4j/bin/neo4j"
memgraph_path="/home/mg/david/memgraph/build/memgraph"
num_workers=12
dataset="medium"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -n|--neo4j-path)
            neo4j_path="$2"
            shift
            shift
            ;;
        -m|--memgraph-path)
            memgraph_path="$2"
            shift
            shift
            ;;
        -w|--num-workers)
            num_workers="$2"
            shift
            shift
            ;;
        -d|--dataset)
            dataset="$2"
            shift
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Invalid option: $1"
            show_help
            ;;
    esac
done

# Run Python: Mgbench - Neo4j
echo "Running Python: Mgbench - Neo4j"
python3 /home/mg/david/memgraph/tests/mgbench/benchmark.py \
    vendor-native \
    --vendor-binary "$neo4j_path" \
    --vendor-name neo4j \
    --num-workers-for-benchmark "$num_workers" \
    --num-workers-for-import "$num_workers" \
    --no-load-query-counts \
    --export-results "pokec_${dataset}_results/neo4j_${dataset}_pokec.json" \
    "pokec_disk/${dataset}/*/*" \
    --vendor-specific "config=/usr/share/neo4j/conf/neo4j.conf" \
    --no-authorization

# Run Python: Mgbench - Memgraph - on-disk
echo "Running Python: Mgbench - Memgraph - on-disk"
python3 /home/mg/david/memgraph/tests/mgbench/benchmark.py \
    vendor-native \
    --vendor-binary "$memgraph_path" \
    --vendor-name memgraph \
    --num-workers-for-benchmark "$num_workers" \
    --num-workers-for-import "$num_workers" \
    --no-load-query-counts \
    --export-results-on-disk-txn "pokec_${dataset}_results/on_disk_${dataset}_pokec.json" \
    --export-results "pokec_${dataset}_results/on_disk_export_${dataset}_pokec.json" \
    "pokec_disk/${dataset}/*/*" \
    --no-authorization \
    --vendor-specific "data-directory=/home/mg/david/memgraph/tests/mgbench/datadir1 storage-mode=ON_DISK_TRANSACTIONAL"
