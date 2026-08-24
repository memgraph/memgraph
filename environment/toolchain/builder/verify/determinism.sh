#!/bin/bash
# Build the toolchain twice and report how much of it is not reproducible.
#
# This is a measurement, not a gate. Building GCC and LLVM bit-for-bit
# reproducibly is a project in its own right, and making it a requirement here
# would swallow the port. The number tells us whether it is worth attempting
# later, and whether a future change made things worse.
#
# The archive itself is built deterministically regardless -- sorted order,
# fixed timestamps and ownership, gzip without its own header stamp -- so any
# difference reported here comes from the toolchain contents, not the packaging.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${TC_DETERMINISM_DIR:-/tmp/tc-determinism}"

rm -rf "$WORK"
mkdir -p "$WORK/a" "$WORK/b"

for run in a b; do
    echo "== build $run"
    docker buildx build "$HERE" \
        --target artifact \
        --output "type=local,dest=$WORK/$run" \
        --no-cache-filter s-package
done

a=$(find "$WORK/a" -name '*.tar.gz' | head -1)
b=$(find "$WORK/b" -name '*.tar.gz' | head -1)

echo
echo "== archives"
echo "  a: $(sha256sum "$a" | cut -d' ' -f1)"
echo "  b: $(sha256sum "$b" | cut -d' ' -f1)"
if cmp -s "$a" "$b"; then
    echo "  archives are identical"
    exit 0
fi

echo
echo "== unpacking to compare contents"
mkdir -p "$WORK/xa" "$WORK/xb"
tar -xzf "$a" -C "$WORK/xa"
tar -xzf "$b" -C "$WORK/xb"

total=0
differ=0
while IFS= read -r rel; do
    total=$((total + 1))
    if ! cmp -s "$WORK/xa/$rel" "$WORK/xb/$rel" 2>/dev/null; then
        differ=$((differ + 1))
        echo "  differs: $rel"
    fi
done < <(cd "$WORK/xa" && find . -type f | LC_ALL=C sort)

echo
echo "== result"
echo "  $differ of $total files differ"
awk -v d="$differ" -v t="$total" 'BEGIN { if (t > 0) printf "  %.2f%% not reproducible\n", 100*d/t }'
