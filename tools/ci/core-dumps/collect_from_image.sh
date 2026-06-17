#!/usr/bin/env bash
# Collect a core dump from a crashed container whose main process (PID 1) is
# memgraph — e.g. a memgraph/MAGE docker image run during e2e tests.
#
# Because memgraph is PID 1, a crash exits the container, so it must be started
# WITHOUT --rm (the exited container then persists and we can copy the core out
# of it). The crashed container can't be `docker exec`-ed (it's stopped), so we
# analyze the core in a short-lived "analyzer" container started from the SAME
# image — which carries the matching binary, and (for the debug/relwithdebinfo
# image) gdb + debug symbols. Everything else is delegated to collect.sh.
#
# Best-effort: never fail the CI job.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

SOURCE_CONTAINER=""
IMAGE=""
RUN_ID=""
JOB_ID=""
BINARY="/usr/lib/memgraph/memgraph"
BUILD_DIR="/usr/lib/memgraph"
CORES_DIR="/tmp/mg-cores"
UPLOAD_CORE="auto"
CORE_SIZE_LIMIT="2"
ANALYZER_CONTAINER="mg-core-analyzer"

print_usage() {
  cat <<EOF
Usage: collect_from_image.sh --source-container NAME --image IMAGE --run-id ID --job-id ID [OPTIONS]

Options:
  --source-container NM Crashed (exited) container to copy the core out of (required)
  --image IMAGE         Image to start the throwaway analyzer from (required;
                        use the debug/relwithdebinfo image — it has gdb + symbols)
  --run-id ID           Workflow run id (required)
  --job-id ID           Job identifier (required)
  --binary PATH         Memgraph binary inside the image (default: $BINARY)
  --build-dir DIR       Dir to bundle as binaries.tar.gz (default: $BUILD_DIR)
  --cores-dir DIR       Core dump dir inside the container (default: $CORES_DIR)
  --upload-core MODE    true | false | auto (default: $UPLOAD_CORE)
  --core-size-limit N   auto threshold in GiB (default: $CORE_SIZE_LIMIT)
  -h, --help            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-container) SOURCE_CONTAINER="$2"; shift 2 ;;
    --image)            IMAGE="$2"; shift 2 ;;
    --run-id)           RUN_ID="$2"; shift 2 ;;
    --job-id)           JOB_ID="$2"; shift 2 ;;
    --binary)           BINARY="$2"; shift 2 ;;
    --build-dir)        BUILD_DIR="$2"; shift 2 ;;
    --cores-dir)        CORES_DIR="$2"; shift 2 ;;
    --upload-core)      UPLOAD_CORE="$2"; shift 2 ;;
    --core-size-limit)  CORE_SIZE_LIMIT="$2"; shift 2 ;;
    -h|--help)          print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$SOURCE_CONTAINER" || -z "$IMAGE" || -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --source-container, --image, --run-id and --job-id are required" >&2
  exit 1
fi

if ! docker inspect "$SOURCE_CONTAINER" >/dev/null 2>&1; then
  echo "Container '$SOURCE_CONTAINER' not found — nothing to collect."
  exit 0
fi

# Copy the cores out of the crashed container (works whether it is exited or
# still running).
host_cores="$(mktemp -d)"
docker cp "${SOURCE_CONTAINER}:${CORES_DIR}/." "$host_cores" 2>/dev/null || true
shopt -s nullglob
cores=("$host_cores"/core.*)
shopt -u nullglob
if [[ ${#cores[@]} -eq 0 ]]; then
  echo "No core dumps in ${SOURCE_CONTAINER}:${CORES_DIR} — nothing to collect."
  rm -rf "$host_cores"
  exit 0
fi
echo "Copied ${#cores[@]} core dump(s) from ${SOURCE_CONTAINER}."

# Start a throwaway analyzer from the same image, with the cores bind-mounted in
# at the expected path. sleep keeps it alive so collect.sh can docker exec it.
docker rm -f "$ANALYZER_CONTAINER" >/dev/null 2>&1 || true
if ! docker run -d --name "$ANALYZER_CONTAINER" --entrypoint sleep \
       -v "${host_cores}:${CORES_DIR}" "$IMAGE" infinity >/dev/null 2>&1; then
  echo "Warning: could not start analyzer container from ${IMAGE} (continuing)." >&2
  rm -rf "$host_cores"
  exit 0
fi

# Delegate to collect.sh: analyze (gdb is in the debug image), upload the trace,
# the core and the binary, and ping monitoring. Run everything as root — the
# copied cores are root-owned and the image runs as the unprivileged memgraph
# user otherwise.
"$SCRIPT_DIR/collect.sh" \
  --build-container "$ANALYZER_CONTAINER" \
  --binary "$BINARY" \
  --build-dir "$BUILD_DIR" \
  --cores-dir "$CORES_DIR" \
  --exec-user root \
  --run-id "$RUN_ID" \
  --job-id "$JOB_ID" \
  --upload-core "$UPLOAD_CORE" \
  --core-size-limit "$CORE_SIZE_LIMIT" \
  || echo "Warning: collect.sh exited non-zero (continuing)." >&2

docker rm -f "$ANALYZER_CONTAINER" >/dev/null 2>&1 || true
rm -rf "$host_cores"
