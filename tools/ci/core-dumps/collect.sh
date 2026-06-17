#!/usr/bin/env bash
# Host-side orchestrator for Memgraph CI core-dump handling.
#
# Intended to be called from a workflow's `if: failure()` step. It:
#   1. checks the test container for core dumps in /tmp/mg-cores,
#   2. analyzes them with gdb INSIDE the container (analyze_core_dumps.sh),
#   3. copies the resulting stack traces out to the host,
#   4. uploads them to S3 and pings monitoring (upload_stack_trace.sh).
#
# It never fails hard — a problem here should not turn a real failure into a
# confusing one — so the caller can additionally use continue-on-error.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

TOOLCHAIN="v7"
OS=""
ARCH=""
RUN_ID=""
JOB_ID=""
BUILD_CONTAINER=""
CORES_DIR="/tmp/mg-cores"
BINARY="/home/mg/memgraph/build/memgraph"
BUILD_DIR="/home/mg/memgraph/build"
MGBUILD_ROOT_DIR="/home/mg/memgraph"
UPLOAD_CORE=false

print_usage() {
  cat <<EOF
Usage: collect.sh --run-id ID --job-id ID [OPTIONS]

Options:
  --toolchain VER       Toolchain version (default: $TOOLCHAIN)
  --os NAME             Operating system (used to derive the container name)
  --arch NAME           Architecture (accepted for symmetry; unused for name)
  --run-id ID           Workflow run id (required)
  --job-id ID           Job identifier (required)
  --build-container NM  Override the container name (default: mgbuild_<tc>_<os>)
  --cores-dir DIR       Core dump dir inside the container (default: $CORES_DIR)
  --binary PATH         Memgraph binary inside the container (default: $BINARY)
  --build-dir DIR       Build dir inside the container (default: $BUILD_DIR)
  --upload-core         Also upload the core + build artifacts to S3 (ci-cores/)
  -h, --help            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --toolchain)       TOOLCHAIN="$2"; shift 2 ;;
    --os)              OS="$2"; shift 2 ;;
    --arch)            ARCH="$2"; shift 2 ;;  # shellcheck disable=SC2034 # accepted for symmetry
    --run-id)          RUN_ID="$2"; shift 2 ;;
    --job-id)          JOB_ID="$2"; shift 2 ;;
    --build-container) BUILD_CONTAINER="$2"; shift 2 ;;
    --cores-dir)       CORES_DIR="$2"; shift 2 ;;
    --binary)          BINARY="$2"; shift 2 ;;
    --build-dir)       BUILD_DIR="$2"; shift 2 ;;
    --upload-core)     UPLOAD_CORE=true; shift 1 ;;
    -h|--help)         print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --run-id and --job-id are required" >&2
  exit 1
fi

if [[ -z "$BUILD_CONTAINER" ]]; then
  if [[ -z "$OS" ]]; then
    echo "Error: provide --os (or --build-container) to locate the container" >&2
    exit 1
  fi
  BUILD_CONTAINER="mgbuild_${TOOLCHAIN}_${OS}"
fi

if ! docker inspect "$BUILD_CONTAINER" >/dev/null 2>&1; then
  echo "Container '$BUILD_CONTAINER' not found — skipping core dump collection."
  exit 0
fi

# Any core dumps to handle?
core_count="$(docker exec "$BUILD_CONTAINER" bash -c \
  "ls -1 ${CORES_DIR}/core.* 2>/dev/null | wc -l" 2>/dev/null || echo 0)"
if [[ "${core_count:-0}" -eq 0 ]]; then
  echo "No core dumps found in ${BUILD_CONTAINER}:${CORES_DIR} — nothing to collect."
  exit 0
fi
echo "Found $core_count core dump(s) in ${BUILD_CONTAINER}:${CORES_DIR}."

container_out="${CORES_DIR}/stacktraces"
analyze_script="${MGBUILD_ROOT_DIR}/tools/ci/core-dumps/analyze_core_dumps.sh"

# Analyze inside the container as the mg user (binary + cores live there, and
# gdb comes from the toolchain).
docker exec -u mg "$BUILD_CONTAINER" bash -c \
  "bash '$analyze_script' --cores-dir '$CORES_DIR' --binary '$BINARY' --out-dir '$container_out' --toolchain '$TOOLCHAIN'" \
  || echo "Warning: analyze step exited non-zero (continuing)." >&2

# Copy the produced stack traces out to a host temp dir.
host_out="$(mktemp -d)"
if ! docker cp "${BUILD_CONTAINER}:${container_out}/." "$host_out" 2>/dev/null; then
  echo "Warning: no stack traces to copy from the container." >&2
  rm -rf "$host_out"
  exit 0
fi

"$SCRIPT_DIR/upload_stack_trace.sh" \
  --traces-dir "$host_out" \
  --run-id "$RUN_ID" \
  --job-id "$JOB_ID" \
  || echo "Warning: upload step exited non-zero (continuing)." >&2

rm -rf "$host_out"

# Optionally upload the raw core + build artifacts for offline gdb analysis.
if [[ "$UPLOAD_CORE" == true ]]; then
  "$SCRIPT_DIR/upload_core.sh" \
    --build-container "$BUILD_CONTAINER" \
    --cores-dir "$CORES_DIR" \
    --build-dir "$BUILD_DIR" \
    --run-id "$RUN_ID" \
    --job-id "$JOB_ID" \
    || echo "Warning: core upload step exited non-zero (continuing)." >&2
fi
