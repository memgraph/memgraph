#!/bin/bash
# relocate: rewrite absolute runpaths inside the prefix to $ORIGIN-relative ones.
#
# Every build system here spells $ORIGIN differently, and several eat it: make
# reads a bare $O as a variable and leaves RIGIN behind, a recursive build eats
# it again, and configure adds another layer. Chasing that through each recipe
# produced one binary with a literal RIGIN and another with a runpath resolved
# against the caller's working directory. Doing it once, afterwards, on the
# finished tree, is both uniform and checkable.
#
# Only paths inside the prefix are touched; anything else is left exactly as it
# was, because a runpath pointing outside the tree is a different problem and
# not one to paper over here. Files are rewritten only when the value actually
# changes, so the archive stays byte-identical where nothing needed doing.
set -euo pipefail
source /tc/lib/common.sh

log_tool_name "relocate (runpaths -> \$ORIGIN)"

changed=0
scanned=0
while IFS= read -r f; do
    # patchelf refuses non-ELF input; ask first rather than filtering by name.
    rp=$(patchelf --print-rpath "$f" 2>/dev/null) || continue
    [[ -z "$rp" ]] && continue
    scanned=$((scanned + 1))

    dir=$(dirname "$f")
    new=""
    for entry in ${rp//:/ }; do
        case "$entry" in
            "$PREFIX"/*|"$PREFIX")
                rel=$(realpath -m --relative-to="$dir" "$entry")
                new="${new:+$new:}\$ORIGIN/$rel"
                ;;
            *)
                new="${new:+$new:}$entry"
                ;;
        esac
    done

    if [[ "$new" != "$rp" ]]; then
        patchelf --set-rpath "$new" "$f"
        changed=$((changed + 1))
    fi
done < <(find "$PREFIX" -type f -not -name '*.a' -not -name '*.o' 2>/dev/null)

echo "  runpaths: $scanned carried one, $changed rewritten"

# A rewrite that leaves something unable to load is worse than the absolute path
# it replaced, so check before the archive is built rather than after it ships.
bad=0
while IFS= read -r f; do
    file "$f" 2>/dev/null | grep -q 'ELF.*dynamically linked' || continue
    if ldd "$f" 2>&1 | grep -q 'not found'; then
        echo "  UNRESOLVED after rewrite: ${f#"$PREFIX"/}"
        bad=$((bad + 1))
    fi
done < <(find "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/lib64" "$PREFIX/libexec" -type f 2>/dev/null)

if (( bad )); then
    echo "  $bad file(s) cannot resolve their libraries after rewriting" >&2
    exit 1
fi
echo "  every rewritten file still resolves"
