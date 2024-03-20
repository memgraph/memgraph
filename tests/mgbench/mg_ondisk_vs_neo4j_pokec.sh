#!/bin/bash

# Currently only pokec dataset is modified to be used with memgraph on-disk storage

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd "$SCRIPT_DIR"

# Help function
function show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -n, --neo4j-path     Path to Neo4j binary"
    echo "  -m, --memgraph-path  Path to Memgraph binary"
    echo "  -w, --num-workers    Number of workers for benchmark and import"
    echo "  -d, --dataset_size   dataset_size (small, medium, large)"
    echo "  -h, --help           Show this help message"
    exit 0
}

# Default values
neo4j_path="/usr/share/neo4j/bin/neo4j"
memgraph_path="../../build/memgraph"
num_workers=12
dataset_size="small"

if [! -d "pokec_${dataset_size}_results" ]; then
    mkdir "pokec_${dataset_size}_results"
fi

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
        -d|--dataset_size)
            dataset_size="$2"
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
python3  benchmark.py vendor-native \
    --vendor-binary "$neo4j_path" \
    --vendor-name neo4j \
    --num-workers-for-benchmark "$num_workers" \
    --num-workers-for-import "$num_workers" \
    --no-load-query-counts \
    --export-results "pokec_${dataset_size}_results/neo4j_${dataset_size}_pokec.json" \
    "pokec_disk/${dataset_size}/*/*" \
    --vendor-specific "config=$neo4j_path/conf/neo4j.conf" \
    --no-authorization

# Run Python: Mgbench - Memgraph - on-disk
echo "Running Python: Mgbench - Memgraph - on-disk"
python3 benchmark.py vendor-native \
    --vendor-binary "$memgraph_path" \
    --vendor-name memgraph \
    --num-workers-for-benchmark "$num_workers" \
    --num-workers-for-import "$num_workers" \
    --no-load-query-counts \
    --export-results-on-disk-txn "pokec_${dataset_size}_results/on_disk_${dataset_size}_pokec.json" \
    --export-results "pokec_${dataset_size}_results/on_disk_export_${dataset_size}_pokec.json" \
    "pokec_disk/${dataset_size}/*/*" \
    --no-authorization \
    --vendor-specific "data-directory=benchmark_datadir" "storage-mode=ON_DISK_TRANSACTIONAL"
