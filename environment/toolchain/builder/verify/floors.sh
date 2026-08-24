#!/bin/bash
# Fail the build if anything the toolchain ships, or anything the toolchain
# produces, needs a newer glibc than the floor it declares.
#
# There are two floors and they are independent. The toolchain's own binaries
# link against the build container's glibc, which decides where the toolchain
# can run. The binaries the toolchain compiles link against the sysroot glibc,
# which decides where memgraph can run. Both are silent failures otherwise:
# nothing errors at build time, and the break surfaces on an older machine.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/glibc.env"
source "$TC_VERSIONS/linux-headers.env"

# The oldest kernel the toolchain promises to run on, and to produce binaries
# for. Derived the same way the glibc stage derives it, so the promise and the
# thing that implements it cannot drift apart.
KERNEL_FLOOR="${LINUX_HEADERS_VERSION%.*}"

# The highest glibc symbol version the toolchain's own binaries may require.
# Set by the base image, so it moves only when the base image does.
BUILDER_GLIBC_FLOOR="${TC_BUILDER_GLIBC_FLOOR:?TC_BUILDER_GLIBC_FLOOR must be set}"

# Files already above the floor when this check was introduced. binutils builds
# gprofng's collector libraries against the host glibc instead of the sysroot,
# so they reference the 2.32/2.34 pthread and dl consolidations and will not
# load on an older target. Everything else in the toolchain respects the floor.
# Fixing the binutils build removes these; until then they are recorded here so
# the check stays enforcing for everything else rather than being switched off.
KNOWN_ABOVE_FLOOR=(
    "lib/gprofng/libgp-collector.so"
    "lib/gprofng/libgp-collectorAPI.so"
    "lib/gprofng/libgp-heap.so"
    "lib/gprofng/libgp-iotrace.so"
    "lib/gprofng/libgp-sync.so"
)

is_known() {
    local rel="${1#"$PREFIX"/}"
    local k
    for k in "${KNOWN_ABOVE_FLOOR[@]}"; do
        [[ "$rel" == "$k" ]] && return 0
    done
    return 1
}

ver_gt() { [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" == "$1" && "$1" != "$2" ]]; }

max_glibc_required() {
    # Highest GLIBC_x.y symbol version referenced by one ELF file.
    objdump -T "$1" 2>/dev/null | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' \
        | sort -V | tail -1 || true
}

status=0
still_known=()

echo "== toolchain binaries must run on glibc $BUILDER_GLIBC_FLOOR"
worst=""
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    need=$(max_glibc_required "$f")
    if [[ -z "$need" ]]; then continue; fi
    if ver_gt "$need" "$BUILDER_GLIBC_FLOOR"; then
        if is_known "$f"; then
            echo "  known  ${f#"$PREFIX"/} needs glibc $need"
            still_known+=("${f#"$PREFIX"/}")
        else
            echo "  FAIL $f needs glibc $need"
            status=1
        fi
    elif is_known "$f"; then
        echo "  stale exemption: ${f#"$PREFIX"/} is within the floor now; drop it"
        status=1
    fi
    if [[ -z "$worst" ]] || ver_gt "$need" "$worst"; then worst="$need"; fi
done < <(find "$PREFIX" -type f -executable -not -path "$SYSROOT/*")
echo "  highest required: ${worst:-none} (${#still_known[@]} known exemptions)"

echo "== a binary compiled by the toolchain must run on glibc $GLIBC_VERSION"
probe=$(mktemp -d)
cat > "$probe/probe.cpp" <<'EOF'
#include <string>
#include <iostream>
int main() { std::string s = "floor"; std::cout << s << "\n"; return 0; }
EOF
"$PREFIX/bin/clang++" --sysroot="$SYSROOT" --gcc-toolchain="$PREFIX" \
    -std=c++20 -O1 -o "$probe/probe" "$probe/probe.cpp"
need=$(max_glibc_required "$probe/probe")
echo "  probe requires glibc ${need:-none}"
if [[ -n "$need" ]] && ver_gt "$need" "$GLIBC_VERSION"; then
    echo "  FAIL compiled binaries need glibc $need, sysroot floor is $GLIBC_VERSION"
    status=1
fi

# --------------------------------------------------------------------------
# Kernel. An ELF executable records the oldest kernel it will run on in its
# .note.ABI-tag; glibc's --enable-kernel is what puts it there. Requiring a
# newer kernel than the floor is a break, and a silent one: the binary simply
# fails to start on a supported machine.
#
# Shared objects carry no such note, so an absent note is not a finding. A note
# *older* than the floor is not a break either -- it runs in more places -- but
# it does mean the link did not pick up the sysroot's startup files, so it is
# worth seeing rather than passing over.
# --------------------------------------------------------------------------

min_kernel() {
    readelf -n "$1" 2>/dev/null \
        | grep -oE 'ABI: [0-9]+\.[0-9]+\.[0-9]+' | head -1 | sed 's/ABI: //' || true
}

echo "== nothing may require a kernel newer than $KERNEL_FLOOR"
permissive=0
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    k=$(min_kernel "$f")
    if [[ -z "$k" ]]; then continue; fi
    if ver_gt "$k" "$KERNEL_FLOOR.0"; then
        echo "  FAIL ${f#"$PREFIX"/} needs kernel $k"
        status=1
    elif ver_gt "$KERNEL_FLOOR.0" "$k"; then
        permissive=$((permissive + 1))
    fi
done < <(find "$PREFIX" -type f -executable -not -path "$SYSROOT/*")
echo "  $permissive file(s) declare an older kernel than the floor (linked without the sysroot's startup files)"

echo "== a binary compiled by the toolchain must run on kernel $KERNEL_FLOOR"
k=$(min_kernel "$probe/probe")
echo "  probe requires kernel ${k:-none}"
if [[ -n "$k" ]] && ver_gt "$k" "$KERNEL_FLOOR.0"; then
    echo "  FAIL compiled binaries need kernel $k, floor is $KERNEL_FLOOR"
    status=1
fi

rm -rf "$probe"

if [[ $status -ne 0 ]]; then
    echo "floor check FAILED"
    exit 1
fi
echo "floor check passed (glibc $GLIBC_VERSION / $BUILDER_GLIBC_FLOOR, kernel $KERNEL_FLOOR)"
