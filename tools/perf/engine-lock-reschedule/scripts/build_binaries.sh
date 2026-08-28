#!/usr/bin/env bash
# Build the six memgraph binaries (master + the five PR branches) into relocatable bundles under
# bins/<label>/ so the phase-3 grid can A/B them. Each bundle is { memgraph, src/query/*.so } because
# the binary's RUNPATH is $ORIGIN/src/query -- the query .so's MUST sit beside the binary or it won't
# start. The script verifies each relocated bundle actually runs (`--version`) before moving on.
#
# One git worktree per branch is created under $WT_ROOT, built RelWithDebInfo, stashed, removed.
# ccache warm across builds makes this far cheaper than six cold builds.
#
# Usage:   ./build_binaries.sh            # builds all six
#          BUILDS="master:origin/master p4669:perf/begin-engine-lock-tryresched" ./build_binaries.sh
# Env:     TOOLCHAIN=/opt/toolchain-v8  REPO=<path to a memgraph checkout>  WT_ROOT=/tmp/mg-perf-wt
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BINS_DIR="$HERE/../bins"; mkdir -p "$BINS_DIR"
TOOLCHAIN="${TOOLCHAIN:-/opt/toolchain-v8}"
REPO="${REPO:-$(git -C "$HERE" rev-parse --show-toplevel)}"
WT_ROOT="${WT_ROOT:-/tmp/mg-perf-wt}"; mkdir -p "$WT_ROOT"

# label:branch  (branches must be fetched; master baseline uses origin/master)
BUILDS="${BUILDS:-master:origin/master \
p4662:perf/query-timeout-deadline \
p4663:perf/adaptive-worker-spin \
p4668:perf/read-commit-high-priority \
p4669:perf/begin-engine-lock-tryresched \
p4684:perf/prepare-reschedule}"

git -C "$REPO" fetch origin --quiet 2>/dev/null || true

for spec in $BUILDS; do
  label="${spec%%:*}"; branch="${spec#*:}"; wt="$WT_ROOT/$label"
  echo "===== [$label] building $branch  $(date -u +%H:%M:%S) ====="
  git -C "$REPO" worktree remove --force "$wt" 2>/dev/null || true
  if ! git -C "$REPO" worktree add --force "$wt" "$branch" >/dev/null 2>&1; then
    echo "[$label] WORKTREE_FAIL ($branch not found? fetch it)"; continue
  fi
  ( cd "$wt"
    source "$TOOLCHAIN/activate"
    export CC="$TOOLCHAIN/bin/clang" CXX="$TOOLCHAIN/bin/clang++" MG_TOOLCHAIN_ROOT="$TOOLCHAIN"
    bash build.sh --build-type RelWithDebInfo --skip-os-deps --target memgraph
  ) > "$WT_ROOT/$label.build.log" 2>&1
  if [ ! -x "$wt/build/memgraph" ] || grep -qE "FAILED:|ninja: build stopped|features.h" "$WT_ROOT/$label.build.log"; then
    echo "[$label] BUILD_FAIL -- see $WT_ROOT/$label.build.log"; continue
  fi
  # stash relocatable bundle: binary + the query .so's it RUNPATHs to ($ORIGIN/src/query)
  rm -rf "$BINS_DIR/$label"; mkdir -p "$BINS_DIR/$label/src/query"
  cp "$wt/build/memgraph" "$BINS_DIR/$label/memgraph"
  cp "$wt"/build/src/query/*.so "$BINS_DIR/$label/src/query/" 2>/dev/null || true
  # verify the RELOCATED bundle runs before we trust it (stale/missing .so shows up here)
  ver=$(LD_LIBRARY_PATH="$TOOLCHAIN/lib:$TOOLCHAIN/lib64" "$BINS_DIR/$label/memgraph" --version 2>/dev/null | head -1)
  if [ -n "$ver" ]; then echo "[$label] OK  $ver  ($(git -C "$wt" rev-parse --short HEAD))"
  else echo "[$label] RELOCATION_FAIL -- bundle does not run"; fi
  git -C "$REPO" worktree remove --force "$wt" 2>/dev/null || true
done
echo "=== BUILD_BINARIES DONE -- bundles in $BINS_DIR ==="
ls -d "$BINS_DIR"/*/ 2>/dev/null
