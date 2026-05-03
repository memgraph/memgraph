#!/usr/bin/env bash
# Detect and remove inconsistent entries from the local Conan 2 cache.
#
# After previous CI runs that died mid-extraction (OOM, SIGBUS, etc.) the
# cache index can list a package as available while its folder is half-
# written or absent, and the next `conan install` aborts with:
#
#   AssertionError: Pkg '<ref>' folder must exist: ~/.conan2/p/<hash>/p
#
# Run this script before `conan install` to remove broken entries so they
# get re-fetched cleanly. Cheap to run; `conan cache check-integrity` is
# fast on a populated cache.
#
# Exits 0 always: missing/empty cache is fine (nothing to repair), and
# unrepairable corruption falls through to the actual conan install which
# will surface the real error.

set -uo pipefail

if ! command -v conan >/dev/null; then
    echo "[conan-doctor] conan not on PATH; skipping" >&2
    exit 0
fi

echo "[conan-doctor] running cache check-integrity"
report=$(conan cache check-integrity "*" 2>&1)
rc=$?
echo "${report}"

if [[ ${rc} -eq 0 ]]; then
    echo "[conan-doctor] cache is consistent"
    exit 0
fi

# Extract refs that failed the check. Conan's output for a broken package
# typically contains "ERROR: <ref>: ..." or the AssertionError pattern.
# Be permissive: anything matching <name>/<ver>[@user/channel][#rrev] gets
# removed.
mapfile -t broken < <(printf '%s\n' "${report}" \
    | grep -oE '[a-zA-Z0-9_.+-]+/[0-9][^[:space:]:]*(@[a-zA-Z0-9_.+/-]+)?(#[0-9a-f]+)?' \
    | sort -u)

if [[ ${#broken[@]} -eq 0 ]]; then
    echo "[conan-doctor] check-integrity reported a problem but no refs parsed; leaving cache as-is"
    exit 0
fi

echo "[conan-doctor] removing broken refs:"
printf '  %s\n' "${broken[@]}"
for ref in "${broken[@]}"; do
    conan remove "${ref}" -c 2>&1 | sed 's/^/[conan-doctor]   /' || true
done
echo "[conan-doctor] done; conan install will re-fetch removed packages"
