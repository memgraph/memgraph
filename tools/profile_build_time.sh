#!/usr/bin/env bash
#
# Reproducible -ftime-trace + ClangBuildAnalyzer profile of a cold memgraph
# build. Run twice on an unchanged tree, compare the outputs with
# tools/profile_build_time_compare.sh.
#
#   tools/profile_build_time.sh [LABEL]
#
# LABEL defaults to a timestamp; it's only used to name output files.
# Outputs land in build_time_results/ at the repo root.
#
# Requires:
#   ClangBuildAnalyzer on PATH (https://github.com/aras-p/ClangBuildAnalyzer),
#   or the CBA env var pointing at an executable binary
#   mgcxx/text_search/CMakeLists.txt with CFLAGS=/CXXFLAGS= wipe around cargo
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Resolve the ClangBuildAnalyzer binary.
#   1. If CBA is set, use it verbatim.
#   2. Otherwise look for `ClangBuildAnalyzer` on PATH.
# No machine-specific default — the path must resolve portably or fail fast.
if [[ -n "${CBA:-}" ]]; then
  :  # user-provided
elif CBA=$(command -v ClangBuildAnalyzer 2>/dev/null); then
  :  # found on PATH
else
  CBA=""
fi

BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
TARGET="${TARGET:-memgraph}"
LABEL="${1:-$(date +%Y%m%d_%H%M%S)}"
RESULTS_DIR="${REPO_ROOT}/build_time_results"

BIN_FILE="${RESULTS_DIR}/build_timings_${LABEL}.bin"
ANALYSIS_FILE="${RESULTS_DIR}/analysis_${LABEL}.txt"
HEADERS_FILE="${RESULTS_DIR}/headers_${LABEL}.txt"
ENV_FILE="${RESULTS_DIR}/env_${LABEL}.txt"
LOG_FILE="${RESULTS_DIR}/build_${LABEL}.log"

log()  { printf '[profile %s] %s\n' "${LABEL}" "$*"; }
die()  { printf '[profile %s] ERROR: %s\n' "${LABEL}" "$*" >&2; exit 1; }

# --- Preflight -------------------------------------------------------------

[[ -n "${CBA}"  ]] || die "ClangBuildAnalyzer not found on PATH. Set CBA=/path/to/ClangBuildAnalyzer or install it from https://github.com/aras-p/ClangBuildAnalyzer."
[[ -f "${CBA}"  ]] || die "CBA=${CBA} does not exist."
[[ -x "${CBA}"  ]] || die "CBA=${CBA} is not executable."

grep -q 'CFLAGS=' mgcxx/text_search/CMakeLists.txt \
  || die "mgcxx/text_search/CMakeLists.txt is missing the CFLAGS=/CXXFLAGS= wipe around the cargo call; -ftime-trace will leak into Rust and break the build."

if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
  log "WARN: working tree is dirty; profile captures uncommitted state"
fi

mkdir -p "${RESULTS_DIR}"

# --- Wipe caches -----------------------------------------------------------

log "wiping build/ and clearing ccache"
rm -rf build
ccache -Cz >/dev/null 2>&1 || true
export CCACHE_DISABLE=1   # belt-and-braces: CMakeLists.txt:19 FORCEs the launcher

# --- Configure -------------------------------------------------------------

log "configuring via ./build.sh (build_type=${BUILD_TYPE})"
./build.sh \
  --build-type "${BUILD_TYPE}" \
  --skip-os-deps \
  --config-only \
  -DMG_ENTERPRISE=ON \
  -DMG_MEMORY_PROFILE=OFF \
  -DCMAKE_CXX_FLAGS="-ftime-trace ${CXXFLAGS:-}" \
  -DCMAKE_C_FLAGS="-ftime-trace ${CFLAGS:-}" \
  2>&1 | tee "${LOG_FILE}.configure"

# build.sh sources conanbuild.sh only for its own configure; our separate
# `cmake --build` invocation below runs in a fresh env, so it would miss
# CLASSPATH — which is needed for antlr4's grammar codegen. Source it here.
# shellcheck disable=SC1091
source build/generators/conanbuild.sh

# --- Baseline, build, capture ---------------------------------------------

log "ClangBuildAnalyzer --start build"
"${CBA}" --start build

log "building target '${TARGET}' with -j ${JOBS} (CCACHE_DISABLE=1)"
cmake --build build --target "${TARGET}" -- -j "${JOBS}" 2>&1 | tee "${LOG_FILE}"

log "ClangBuildAnalyzer --stop build ${BIN_FILE}"
"${CBA}" --stop build "${BIN_FILE}"

# --- Analyse ---------------------------------------------------------------

log "ClangBuildAnalyzer --analyze -> ${ANALYSIS_FILE}"
"${CBA}" --analyze "${BIN_FILE}" | tee "${ANALYSIS_FILE}"

log "tools/analyze_ftime_trace.py top-50 -> ${HEADERS_FILE}"
python3 tools/analyze_ftime_trace.py build 50 >"${HEADERS_FILE}" 2>&1 || true

# --- Fingerprint -----------------------------------------------------------

{
  echo "label:        ${LABEL}"
  echo "repo_root:    ${REPO_ROOT}"
  echo "git_head:     $(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_dirty:    $([[ -n "$(git status --porcelain 2>/dev/null || true)" ]] && echo yes || echo no)"
  echo "build_type:   ${BUILD_TYPE}"
  echo "target:       ${TARGET}"
  echo "jobs:         ${JOBS}"
  echo "nproc:        $(nproc)"
  echo "hostname:     $(hostname)"
  echo "date:         $(date -Iseconds)"
  echo "clang:        $(clang --version 2>/dev/null | head -1 || echo unknown)"
  echo "CC:           ${CC:-}"
  echo "CXX:          ${CXX:-}"
  echo "CFLAGS:       ${CFLAGS:-}"
  echo "CXXFLAGS:     ${CXXFLAGS:-}"
  echo "CCACHE_DISABLE: ${CCACHE_DISABLE:-}"
  echo "PATH_sha1:    $(printf '%s' "${PATH}" | sha1sum | cut -d' ' -f1)"
  echo
  echo "--- ccache -s ---"
  ccache -s 2>/dev/null || true
} >"${ENV_FILE}"

log "done."
log "  bin:      ${BIN_FILE}"
log "  analysis: ${ANALYSIS_FILE}"
log "  headers:  ${HEADERS_FILE}"
log "  env:      ${ENV_FILE}"
