#!/usr/bin/env bash
#
# Profile a memgraph build: peak memory, CPU and wall time of every compile,
# link and custom step, plus machine-wide memory over the whole run. Drives the
# ordinary ./build.sh, so what gets measured is what developers build.
#
#   tools/build_profile/profile.sh [OPTIONS] [BUILD_SH_ARGS...]
#
#   tools/build_profile/profile.sh                                  # full clean build, ccache off
#   tools/build_profile/profile.sh --dev -DMG_ENABLE_TESTING=OFF     # quicker turnaround
#   tools/build_profile/profile.sh --ccache --target memgraph        # keep ccache (numbers for hits are meaningless)
#   tools/build_profile/profile.sh --report-only --out build_profile_results/<run>
#
#   # Wrap a build command of your own instead of ./build.sh (what mgbuild does).
#   # The configure must already have run with MG_BUILD_PROFILE_LOG=DIR/steps.jsonl
#   # in the environment and -DCMAKE_PROJECT_INCLUDE=<here>/launcher.cmake.
#   tools/build_profile/profile.sh --exec --out DIR -- cmake --build --preset conan-release
#
# OPTIONS (everything else is handed to build.sh unchanged, or forms the
# command under --exec):
#   --out DIR          results directory (default: build_profile_results/<timestamp>)
#   --exec             run the command after `--` instead of ./build.sh; requires --out
#   --build-dir DIR    build directory for the report and cleanup (default: build)
#   --ccache           keep ccache active; default sets CCACHE_DISABLE=1 so every step
#                      really runs the compiler
#   --sample-ms N      per-step process-tree memory sampling interval (default 50; 0 = rusage only)
#   --sys-interval S   machine-wide sampling interval in seconds (default 0.5)
#   --top N            rows per report table (default 25)
#   --report-only      skip the build, regenerate the report from --out
#   -h, --help
#
# Outputs in DIR: steps.jsonl (raw per-step log), sysmon.jsonl (machine samples),
# build.log, report.txt, plots.html (interactive charts, open in a browser),
# steps.csv, memory_timeline.csv (machine memory vs. running steps, per sample),
# summary.json, trace.json (open in https://ui.perfetto.dev).
#
# The launcher changes every Ninja command line, so a profiled build and a normal
# one do not share incremental state: expect a full rebuild when switching.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
cd "${REPO_ROOT}"

OUT=""
EXEC=false
BUILD_DIR="${REPO_ROOT}/build"
KEEP_CCACHE=false
SAMPLE_MS=50
SYS_INTERVAL=0.5
TOP=25
REPORT_ONLY=false
BUILD_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --out) OUT="$2"; shift 2 ;;
        --exec) EXEC=true; shift ;;
        --build-dir) BUILD_DIR="$(cd "$2" && pwd)"; shift 2 ;;
        --ccache) KEEP_CCACHE=true; shift ;;
        --sample-ms) SAMPLE_MS="$2"; shift 2 ;;
        --sys-interval) SYS_INTERVAL="$2"; shift 2 ;;
        --top) TOP="$2"; shift 2 ;;
        --report-only) REPORT_ONLY=true; shift ;;
        -h|--help) sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        --) shift; BUILD_ARGS+=("$@"); break ;;
        *) BUILD_ARGS+=("$1"); shift ;;
    esac
done

if [[ -z "${OUT}" ]]; then
    if [[ "${REPORT_ONLY}" = true || "${EXEC}" = true ]]; then
        echo "Error: --report-only and --exec need --out DIR" >&2
        exit 1
    fi
    OUT="build_profile_results/$(date +%Y%m%d_%H%M%S)"
fi
if [[ "${EXEC}" = true && ${#BUILD_ARGS[@]} -eq 0 ]]; then
    echo "Error: --exec needs a command after --" >&2
    exit 1
fi
mkdir -p "${OUT}"
OUT="$(cd "${OUT}" && pwd)"
STEPS="${OUT}/steps.jsonl"
SYSMON="${OUT}/sysmon.jsonl"
BUILD_LOG="${OUT}/build.log"

log() { printf '[build_profile] %s\n' "$*"; }

# Python for the driver's own scripts; the per-step wrapper is resolved by
# launcher.cmake from the same MG_PYTHON that build.sh exports.
PYTHON="${MG_PYTHON:-python3}"

report() {
    log "report -> ${OUT}/report.txt"
    "${PYTHON}" "${HERE}/report.py" \
        --steps "${STEPS}" --sysmon "${SYSMON}" --build-dir "${BUILD_DIR}" --top "${TOP}" \
        --csv "${OUT}/steps.csv" --timeline-csv "${OUT}/memory_timeline.csv" \
        --json "${OUT}/summary.json" --trace "${OUT}/trace.json" \
        | tee "${OUT}/report.txt" || log "no report (no steps recorded?)"
    "${PYTHON}" "${HERE}/plots.py" \
        --steps "${STEPS}" --sysmon "${SYSMON}" --build-dir "${BUILD_DIR}" --out "${OUT}/plots.html" \
        || log "no plots"
    log "raw: ${STEPS}   plots: ${OUT}/plots.html   trace: ${OUT}/trace.json"
}

if [[ "${REPORT_ONLY}" = true ]]; then
    report
    exit 0
fi

if [[ "${KEEP_CCACHE}" = true ]]; then
    CCACHE_MODE="active (memory numbers for cache hits are not compile costs)"
else
    export CCACHE_DISABLE=1
    CCACHE_MODE="disabled"
fi
export MG_BUILD_PROFILE_LOG="${STEPS}"
export MG_BUILD_PROFILE_SAMPLE_MS="${SAMPLE_MS}"

: > "${STEPS}"
: > "${SYSMON}"
RUNNER=build.sh
if [[ "${EXEC}" = true ]]; then
    RUNNER=exec
fi
"${PYTHON}" "${HERE}/meta.py" --log "${STEPS}" --ccache "${CCACHE_MODE}" --runner "${RUNNER}" -- "${BUILD_ARGS[@]}"

"${PYTHON}" "${HERE}/sysmon.py" --log "${SYSMON}" --interval "${SYS_INTERVAL}" &
SYSMON_PID=$!

cleanup() {
    kill "${SYSMON_PID}" 2>/dev/null && wait "${SYSMON_PID}" 2>/dev/null || true
    # Leave no trace in the build directory: a stale CMAKE_PROJECT_INCLUDE would
    # break configures once this file is gone (a branch switch, say).
    if [[ -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
        "${PYTHON}" "${HERE}/cmake_cache.py" forget "${BUILD_DIR}/CMakeCache.txt" CMAKE_PROJECT_INCLUDE
    fi
}
trap cleanup EXIT

log "results -> ${OUT}"
log "ccache ${CCACHE_MODE}; per-step sampling every ${SAMPLE_MS} ms"
set +e
if [[ "${EXEC}" = true ]]; then
    log "running: ${BUILD_ARGS[*]}"
    "${BUILD_ARGS[@]}" 2>&1 | tee "${BUILD_LOG}"
else
    log "running: ./build.sh ${BUILD_ARGS[*]}"
    ./build.sh "${BUILD_ARGS[@]}" "-DCMAKE_PROJECT_INCLUDE=${HERE}/launcher.cmake" 2>&1 | tee "${BUILD_LOG}"
fi
BUILD_STATUS=${PIPESTATUS[0]}
set -e
log "build exited with ${BUILD_STATUS}"

report
exit "${BUILD_STATUS}"
