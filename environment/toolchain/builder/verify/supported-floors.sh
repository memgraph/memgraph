#!/bin/bash
# What glibc and kernel do the distros we package for actually ship?
#
# The toolchain's floors are a promise about where memgraph runs, and the only
# defensible value for them is the oldest thing we support. That set changes,
# so this reads it from SUPPORTED_OS_V8 in release/package/mgbuild.sh rather
# than keeping a second list that can disagree with what CI builds, and reads
# the versions from the distro images rather than from a table that ages.
#
# glibc comes from the image. The kernel is the distro's packaged kernel, not
# the one this machine is running, so it is queried from the repository
# metadata -- a container cannot tell you its distro's kernel any other way.
#
# Usage: supported-floors.sh
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MGBUILD="$REPO_ROOT/release/package/mgbuild.sh"

# distro name -> container image. Only the families we package for.
image_for() {
    case "$1" in
        ubuntu-*)  echo "ubuntu:${1#ubuntu-}" ;;
        debian-*)  echo "debian:${1#debian-}" ;;
        fedora-*)  echo "fedora:${1#fedora-}" ;;
        rocky-*)   echo "rockylinux/rockylinux:${1#rocky-}" ;;
        centos-*)  echo "quay.io/centos/centos:stream${1#centos-}" ;;
        *)         echo "" ;;
    esac
}

if [[ ! -f "$MGBUILD" ]]; then
    echo "cannot find $MGBUILD" >&2
    exit 1
fi

# The arm variants are the same distro, so collapse them.
mapfile -t targets < <(
    sed -n '/^SUPPORTED_OS_V8=(/,/^)/p' "$MGBUILD" \
        | grep -oE '[a-z]+-[0-9.]+' | sed 's/-arm$//' | LC_ALL=C sort -u
)

if [[ ${#targets[@]} -eq 0 ]]; then
    echo "could not parse SUPPORTED_OS_V8 from $MGBUILD" >&2
    exit 1
fi

printf '%-16s %-10s %-10s %s\n' TARGET GLIBC KERNEL IMAGE
min_glibc=""
min_kernel=""
min_glibc_from=""
min_kernel_from=""

for t in "${targets[@]}"; do
    img="$(image_for "$t")"
    if [[ -z "$img" ]]; then
        printf '%-16s %-10s %-10s %s\n' "$t" "?" "?" "(no image mapping)"
        continue
    fi

    read -r g k < <(docker run --rm "$img" sh -c '
        # Take the first two components after the name: a development build
        # reports three ("2.43.9000"), and anchoring on the end picks the
        # wrong two.
        g=$(ldd --version 2>&1 | head -1 | sed -nE "s/.*\) ([0-9]+\.[0-9]+).*/\1/p")
        if command -v apt-get >/dev/null 2>&1; then
            apt-get update -qq >/dev/null 2>&1
            k=$(apt-cache policy linux-image-amd64 linux-image-generic 2>/dev/null \
                | grep Candidate | grep -oE "[0-9]+\.[0-9]+" | head -1)
        else
            k=$( (dnf -q info kernel 2>/dev/null || microdnf -q info kernel 2>/dev/null) \
                | grep -iE "^Version" | head -1 | grep -oE "[0-9]+\.[0-9]+")
        fi
        echo "${g:-?} ${k:-?}"
    ' 2>/dev/null) || { g="?"; k="?"; }

    printf '%-16s %-10s %-10s %s\n' "$t" "${g:-?}" "${k:-?}" "$img"

    if [[ "$g" != "?" && -n "$g" ]]; then
        if [[ -z "$min_glibc" ]] || [[ "$(printf '%s\n%s\n' "$g" "$min_glibc" | sort -V | head -1)" == "$g" ]]; then
            min_glibc="$g"; min_glibc_from="$t"
        fi
    fi
    if [[ "$k" != "?" && -n "$k" ]]; then
        if [[ -z "$min_kernel" ]] || [[ "$(printf '%s\n%s\n' "$k" "$min_kernel" | sort -V | head -1)" == "$k" ]]; then
            min_kernel="$k"; min_kernel_from="$t"
        fi
    fi
done

echo
echo "oldest supported glibc:  ${min_glibc:-unknown}  ($min_glibc_from)"
echo "oldest supported kernel: ${min_kernel:-unknown}  ($min_kernel_from)"
echo
echo "The toolchain's sysroot floors should be no higher than these, and there is"
echo "no reason for them to be lower: building below the oldest distro we support"
echo "buys compatibility nobody asked for and holds the builder base back."
