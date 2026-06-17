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
RUN_ID=""
JOB_ID=""
BUILD_CONTAINER=""
CORES_DIR="/tmp/mg-cores"
BINARY="/home/mg/memgraph/build/memgraph"
BUILD_DIR="/home/mg/memgraph/build"
EXEC_USER="mg"
UPLOAD_CORE="auto"
CORE_SIZE_LIMIT="2"   # GiB

print_usage() {
  cat <<EOF
Usage: collect.sh --run-id ID --job-id ID [OPTIONS]

Options:
  --toolchain VER       Toolchain version (default: $TOOLCHAIN)
  --os NAME             Operating system (used to derive the container name)
  --run-id ID           Workflow run id (required)
  --job-id ID           Job identifier (required)
  --build-container NM  Override the container name (default: mgbuild_<tc>_<os>)
  --cores-dir DIR       Core dump dir inside the container (default: $CORES_DIR)
  --binary PATH         Memgraph binary inside the container (default: $BINARY)
  --build-dir DIR       Build dir inside the container (default: $BUILD_DIR)
  --exec-user USER      Container user to run gdb/tar/core ops as (default: $EXEC_USER)
  --upload-core MODE    Upload core + build artifacts: true|false|auto (default: $UPLOAD_CORE)
                        auto uploads only cores <= --core-size-limit; true uploads
                        cores below a 1 TiB hard ceiling; false never uploads.
  --core-size-limit N   auto upload threshold in GiB (default: $CORE_SIZE_LIMIT)
  -h, --help            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --toolchain)       TOOLCHAIN="$2"; shift 2 ;;
    --os)              OS="$2"; shift 2 ;;
    --run-id)          RUN_ID="$2"; shift 2 ;;
    --job-id)          JOB_ID="$2"; shift 2 ;;
    --build-container) BUILD_CONTAINER="$2"; shift 2 ;;
    --cores-dir)       CORES_DIR="$2"; shift 2 ;;
    --binary)          BINARY="$2"; shift 2 ;;
    --build-dir)       BUILD_DIR="$2"; shift 2 ;;
    --exec-user)       EXEC_USER="$2"; shift 2 ;;
    --upload-core)     UPLOAD_CORE="$2"; shift 2 ;;
    --core-size-limit) CORE_SIZE_LIMIT="$2"; shift 2 ;;
    -h|--help)         print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --run-id and --job-id are required" >&2
  exit 1
fi

case "$UPLOAD_CORE" in
  true|false|auto) ;;
  *) echo "Error: --upload-core must be true, false or auto (got '$UPLOAD_CORE')" >&2; exit 1 ;;
esac

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
core_count="$(docker exec -u "$EXEC_USER" "$BUILD_CONTAINER" bash -c \
  "ls -1 ${CORES_DIR}/core.* 2>/dev/null | wc -l" 2>/dev/null || echo 0)"
if [[ "${core_count:-0}" -eq 0 ]]; then
  echo "No core dumps found in ${BUILD_CONTAINER}:${CORES_DIR} — nothing to collect."
  exit 0
fi
echo "Found $core_count core dump(s) in ${BUILD_CONTAINER}:${CORES_DIR}."

container_out="${CORES_DIR}/stacktraces"

# Copy the analyze script into the container and run gdb there as $EXEC_USER.
# Copying it in (rather than assuming the repo is present) lets this work for
# any container: the mgbuild container, or a runtime image (e.g. the MAGE debug
# image) where gdb + debug symbols are already installed.
docker cp "$SCRIPT_DIR/analyze_core_dumps.sh" "${BUILD_CONTAINER}:/tmp/analyze_core_dumps.sh" >/dev/null 2>&1 \
  || echo "Warning: could not copy analyze script into ${BUILD_CONTAINER}." >&2
docker exec -u "$EXEC_USER" "$BUILD_CONTAINER" bash -c \
  "bash /tmp/analyze_core_dumps.sh --cores-dir '$CORES_DIR' --binary '$BINARY' --out-dir '$container_out' --toolchain '$TOOLCHAIN'" \
  || echo "Warning: analyze step exited non-zero (continuing)." >&2

# Copy the produced stack traces out to a host temp dir.
host_out="$(mktemp -d)"
if ! docker cp "${BUILD_CONTAINER}:${container_out}/." "$host_out" 2>/dev/null; then
  echo "Warning: no stack traces to copy from the container." >&2
  rm -rf "$host_out"
  exit 0
fi

# Shared destination for everything from this crash: stack traces, and (with
# --upload-core) the core dump and build artifacts all land in the same folder.
bucket="deps.memgraph.io"
region="eu-west-1"
if command -v openssl >/dev/null 2>&1; then
  hash="$(openssl rand -hex 8)"
else
  hash="$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fi
s3_prefix="ci-stack-traces/${RUN_ID}/${JOB_ID}/${hash}"

# Optionally upload the raw core + build artifacts first; upload_core.sh applies
# the size policy and reports back (via --url-out) the URLs it actually produced,
# so we can attach them to the monitoring log line emitted below. They stay
# empty when nothing was uploaded (mode=false, or the core exceeded the limit).
core_url=""
binaries_url=""
if [[ "$UPLOAD_CORE" != false ]]; then
  url_out="$(mktemp)"
  "$SCRIPT_DIR/upload_core.sh" \
    --build-container "$BUILD_CONTAINER" \
    --cores-dir "$CORES_DIR" \
    --build-dir "$BUILD_DIR" \
    --s3-prefix "$s3_prefix" \
    --bucket "$bucket" \
    --region "$region" \
    --mode "$UPLOAD_CORE" \
    --core-size-limit "$CORE_SIZE_LIMIT" \
    --exec-user "$EXEC_USER" \
    --url-out "$url_out" \
    || echo "Warning: core upload step exited non-zero (continuing)." >&2
  binaries_url="$(sed -n 's/^binaries_url=//p' "$url_out" 2>/dev/null | head -n1)"
  core_url="$(sed -n 's/^core_url=//p' "$url_out" 2>/dev/null | head -n1)"
  rm -f "$url_out"
fi

stack_args=(--traces-dir "$host_out" --s3-prefix "$s3_prefix" --bucket "$bucket" --region "$region")
[[ -n "$core_url" ]] && stack_args+=(--core-url "$core_url")
[[ -n "$binaries_url" ]] && stack_args+=(--binaries-url "$binaries_url")
"$SCRIPT_DIR/upload_stack_trace.sh" "${stack_args[@]}" \
  || echo "Warning: upload step exited non-zero (continuing)." >&2

rm -rf "$host_out"
