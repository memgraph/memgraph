#!/bin/bash

# Script to generate MCDC coverage report from fuzzer corpus
set -e

CORPUS_DIR="./cov_data"
FUZZER="./build/src/planner/test/fuzz_egraph"
COVERAGE_DIR="./mcdc_coverage_data"
FINAL_PROFDATA="mcdc_coverage.profdata"

# LLVM tools from toolchain
LLVM_PROFDATA="/opt/toolchain-v6/bin/llvm-profdata"
LLVM_COV="/opt/toolchain-v6/bin/llvm-cov"

echo "Generating MCDC coverage report from fuzzer corpus..."

# Create coverage directory
mkdir -p "$COVERAGE_DIR"

# Clean up any existing coverage files
rm -f "$COVERAGE_DIR"/*.profraw
rm -f "$FINAL_PROFDATA"

# Counter for progress
total_files=$(ls "$CORPUS_DIR" | wc -l)
current=0

echo "Found $total_files corpus files to process..."

# Run fuzzer on each corpus file to generate coverage
for input_file in "$CORPUS_DIR"/*; do
    current=$((current + 1))
    filename=$(basename "$input_file")

    if [ $((current % 20)) -eq 0 ] || [ $current -eq $total_files ]; then
        echo "Processing file $current/$total_files: $filename"
    fi

    # Set unique profile output for each run with timeout
    if ! timeout 30s bash -c "LLVM_PROFILE_FILE='$COVERAGE_DIR/mcdc_${filename}.profraw' '$FUZZER' '$input_file'" > /dev/null 2>&1; then
        if [ $? -eq 124 ]; then
            echo "Warning: Timeout on file $filename (skipping)"
        else
            echo "Warning: Error on file $filename (skipping)"
        fi
        rm -f "$COVERAGE_DIR/mcdc_${filename}.profraw"
        continue
    fi
done

echo "Merging MCDC coverage data..."

# Merge all profraw files incrementally to avoid command line limits
profraw_files=("$COVERAGE_DIR"/*.profraw)

if [ ${#profraw_files[@]} -eq 0 ] || [ ! -f "${profraw_files[0]}" ]; then
    echo "No .profraw files found in $COVERAGE_DIR"
    exit 1
fi

# Start with the first file
cp "${profraw_files[0]}" "$FINAL_PROFDATA"
echo "Starting merge with $(basename "${profraw_files[0]}")"

# Merge remaining files one by one
for ((i=1; i<${#profraw_files[@]}; i++)); do
    temp_file=$(mktemp)
    if ! "$LLVM_PROFDATA" merge -sparse "$FINAL_PROFDATA" "${profraw_files[i]}" -o "$temp_file"; then
        echo "Failed to merge $(basename "${profraw_files[i]}")"
        rm -f "$temp_file"
        exit 1
    fi
    mv "$temp_file" "$FINAL_PROFDATA"

    if [ $((i % 50)) -eq 0 ] || [ $i -eq $((${#profraw_files[@]} - 1)) ]; then
        echo "Merged $((i+1))/${#profraw_files[@]} files"
    fi
done

echo "Generating MCDC HTML coverage report..."

# Generate HTML report with MCDC
"$LLVM_COV" show "$FUZZER" -instr-profile="$FINAL_PROFDATA" \
    --format=html \
    --output-dir=mcdc_coverage_html \
    --show-line-counts-or-regions \
    --show-expansions \
    --show-instantiations \
    --show-mcdc

echo "MCDC coverage report generated in mcdc_coverage_html/index.html"
echo "MCDC Summary:"
"$LLVM_COV" report "$FUZZER" -instr-profile="$FINAL_PROFDATA" --show-mcdc-summary

echo ""
echo "Files with MCDC conditions:"
"$LLVM_COV" report "$FUZZER" -instr-profile="$FINAL_PROFDATA" | grep -E "(MC/DC|Conditions)" || echo "No additional MCDC details available"

echo "Done!"
