#!/usr/bin/env bash
#
# Lay the build's .debug sidecars out in a build-id-indexed tree, ready for
# upload to a debuginfod-style symbol server.
#
# Usage:
#   prepare-symbol-archive.sh <build-dir> <output-dir>
#
# The build must have been configured with -DMG_SPLIT_DEBUG=ON. Each .debug
# file is placed as <output-dir>/<XX>/<YYYY...YY>.debug, where the build-id
# is read from the file's .note.gnu.build-id (preserved by
# objcopy --only-keep-debug in cmake/SplitDebug.cmake).
#
# A companion manifest at <output-dir>/manifest.tsv records (build-id, source
# basename) for traceability — the build-id layout strips names by design,
# so this is the only place future-you can answer "which binary did this
# correspond to."

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <build-dir> <output-dir>" >&2
    exit 1
fi

build_dir=$1
output_dir=$2

if [[ ! -d "$build_dir" ]]; then
    echo "Error: build directory does not exist: $build_dir" >&2
    exit 1
fi

staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT

echo "Staging debuginfo install tree at $staging"
cmake --install "$build_dir" --component debuginfo --prefix "$staging" >/dev/null

shopt -s nullglob globstar
debug_files=("$staging"/**/*.debug)
if [[ ${#debug_files[@]} -eq 0 ]]; then
    echo "Error: no .debug files installed. Did you build with -DMG_SPLIT_DEBUG=ON?" >&2
    exit 1
fi

mkdir -p "$output_dir"
manifest="$output_dir/manifest.tsv"
: > "$manifest"

count=0
for debug_file in "${debug_files[@]}"; do
    # readelf -n on a .debug sidecar prints the preserved Build ID note as:
    #   Build ID: <40-hex-chars>
    # Stderr is silenced: readelf warns "Unable to find program interpreter
    # name" on every sidecar (the .debug file has no PT_INTERP), which is
    # expected and not actionable.
    build_id=$(readelf -n "$debug_file" 2>/dev/null | awk '/Build ID:/ { print $3; exit }')
    if [[ -z "$build_id" ]]; then
        echo "Warning: no Build ID note in $debug_file — skipping" >&2
        continue
    fi
    prefix=${build_id:0:2}
    suffix=${build_id:2}
    mkdir -p "$output_dir/$prefix"
    cp "$debug_file" "$output_dir/$prefix/$suffix.debug"
    printf "%s\t%s\n" "$build_id" "$(basename "$debug_file")" >> "$manifest"
    count=$((count + 1))
done

echo "Wrote $count sidecars to $output_dir"
echo "Manifest: $manifest"
