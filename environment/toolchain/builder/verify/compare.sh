#!/bin/bash
# Compare two installed toolchains: does the ported build produce the same
# toolchain as the script it replaced?
#
# Byte-identity is not the target and never was -- the tree is ~20k files
# carrying build paths, timestamps and link-order noise. This compares what was
# installed and how it is configured, which is what decides whether a toolchain
# behaves the same. It does not prove either one works; building memgraph with
# it is what does that.
#
# The two halves fail differently, so they are reported separately. A structural
# difference means a stage installed something the other did not, which is
# almost always a porting bug. A configuration difference means a flag was
# dropped or changed in the move.
#
# Usage: compare.sh <prefix-a> <prefix-b>
set -euo pipefail

A="${1:?usage: compare.sh <prefix-a> <prefix-b>}"
B="${2:?usage: compare.sh <prefix-a> <prefix-b>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

for side in a b; do
    p="$A"; [[ "$side" == b ]] && p="$B"
    [[ -d "$p" ]] || { echo "no such prefix: $p" >&2; exit 1; }
    "$HERE/fingerprint.sh" "$p" > "$WORK/$side.txt"
    # split the fingerprint into the structural half and everything else
    awk '/^### structure$/{s=1;next} /^### /{s=0} s && NF' "$WORK/$side.txt" > "$WORK/$side.files"
    awk '/^### structure$/{s=1;next} /^### /{s=0} !s' "$WORK/$side.txt" > "$WORK/$side.config"
done

echo "==== structure"
a_n=$(wc -l < "$WORK/a.files")
b_n=$(wc -l < "$WORK/b.files")
echo "  $A: $a_n files"
echo "  $B: $b_n files"
if diff -q "$WORK/a.files" "$WORK/b.files" >/dev/null; then
    echo "  identical"
    struct=0
else
    only_a=$(LC_ALL=C comm -23 "$WORK/a.files" "$WORK/b.files" | wc -l)
    only_b=$(LC_ALL=C comm -13 "$WORK/a.files" "$WORK/b.files" | wc -l)
    echo "  $only_a only in $A, $only_b only in $B"
    LC_ALL=C comm -23 "$WORK/a.files" "$WORK/b.files" | head -40 | sed 's/^/    only in A: /'
    LC_ALL=C comm -13 "$WORK/a.files" "$WORK/b.files" | head -40 | sed 's/^/    only in B: /'
    [[ $((only_a + only_b)) -gt 80 ]] && echo "    (truncated)"
    struct=1
fi

echo
echo "==== configuration"
if diff -q "$WORK/a.config" "$WORK/b.config" >/dev/null; then
    echo "  identical"
    conf=0
else
    diff -u "$WORK/a.config" "$WORK/b.config" | sed -n '3,60p' | sed 's/^/  /'
    conf=1
fi

echo
if [[ $struct -eq 0 && $conf -eq 0 ]]; then
    echo "equivalent"
    exit 0
fi
echo "NOT equivalent (structure differs: $struct, configuration differs: $conf)"
exit 1
