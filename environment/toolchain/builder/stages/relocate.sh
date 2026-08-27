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

# Both checks walk the same tree the rewrite does. Scanning only bin and the
# library directories would have missed the sysroot, where most of the rewritten
# runpaths actually live -- python's extension modules alone are sixty of them --
# and a break there surfaces as gdb failing to start rather than as a build
# failure.
#
# What could not resolve before any of this, so the check below reports what
# this pass broke rather than what it merely noticed. libabigail's tools are in
# here: they need libdw and libelf, which live in the sysroot, and their runpath
# has never named it. That is a real defect and a separate one.
declare -A was_broken=()
while IFS= read -r f; do
    file "$f" 2>/dev/null | grep -q 'ELF.*dynamically linked' || continue
    ldd "$f" 2>&1 | grep -q 'not found' && was_broken["$f"]=1
done < <(find "$PREFIX" -type f -not -name '*.a' -not -name '*.o' 2>/dev/null)
echo "  unresolved before rewriting: ${#was_broken[@]}"

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
# Only a file that resolved before and does not now is this pass's doing.
broke=0
still=0
while IFS= read -r f; do
    file "$f" 2>/dev/null | grep -q 'ELF.*dynamically linked' || continue
    ldd "$f" 2>&1 | grep -q 'not found' || continue
    if [[ -n "${was_broken[$f]:-}" ]]; then
        still=$((still + 1))
    else
        echo "  BROKEN BY REWRITE: ${f#"$PREFIX"/}"
        broke=$((broke + 1))
    fi
done < <(find "$PREFIX" -type f -not -name '*.a' -not -name '*.o' 2>/dev/null)

if (( broke )); then
    echo "  $broke file(s) resolved before the rewrite and do not now" >&2
    exit 1
fi
echo "  nothing that resolved before stopped resolving ($still already could not)"

# Runpaths are ELF. A path written into a text file is not, and nothing above
# touches it: it keeps naming the machine the toolchain was built on, and the
# tool that reads it fails quietly on every other machine. That is how the gdb
# init file came to name its own install path.
#
# Only the files this build writes itself are checked. A sweep of the whole
# prefix would also catch libtool .la files, pkg-config .pc files and cmake
# export files, which bake absolute paths by design and would have to be
# excused one by one until nobody trusted the check.
echo "== no generated file may name the install path"
baked=0
for rel in etc/gdb/gdbinit activate; do
    f="$PREFIX/$rel"
    [[ -f "$f" ]] || continue
    if grep -qF "$PREFIX" "$f"; then
        echo "  BAKED PATH: $rel"
        baked=$((baked + 1))
    fi
done

if (( baked )); then
    echo "  $baked generated file(s) name $PREFIX; derive the path instead" >&2
    exit 1
fi
echo "  no generated file names the prefix"
