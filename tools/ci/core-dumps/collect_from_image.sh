#!/usr/bin/env bash
# Analyze + upload core dumps produced by a memgraph docker image (memgraph or
# MAGE), where memgraph runs as PID 1.
#
# Two sources of cores are supported:
#   --source-container NAME  Copy the cores out of a crashed (exited) container.
#                            memgraph is PID 1, so the container must have been
#                            started WITHOUT --rm to persist for the copy.
#   --host-cores-dir DIR     Cores already on the host (e.g. the container
#                            bind-mounted /tmp/mg-cores), so no copy is needed —
#                            used where the runtime removes the container at
#                            teardown (e.g. the ha/docker stress deployment).
#
# The crashed container can't be `docker exec`-ed, so we analyze in a short-lived
# "analyzer" container started from the SAME image — which carries the matching
# binary and (for the debug/relwithdebinfo image) gdb + debug symbols. Analysis
# + upload + ping are delegated to collect.sh.
#
# Best-effort: never fail the CI job.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

SOURCE_CONTAINER=""
HOST_CORES_DIR=""
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
Usage: collect_from_image.sh --image IMAGE --run-id ID --job-id ID
                             (--source-container NAME | --host-cores-dir DIR) [OPTIONS]

Options:
  --source-container NM Crashed (exited) container to copy the cores out of
  --host-cores-dir DIR  Host dir already holding the cores (e.g. a bind mount)
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
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-container) SOURCE_CONTAINER="$2"; shift 2 ;;
    --host-cores-dir)   HOST_CORES_DIR="$2"; shift 2 ;;
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

if [[ -z "$IMAGE" || -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --image, --run-id and --job-id are required" >&2
  exit 1
fi
if [[ -z "$SOURCE_CONTAINER" && -z "$HOST_CORES_DIR" ]]; then
  echo "Error: one of --source-container or --host-cores-dir is required" >&2
  exit 1
fi

# Resolve the host dir holding the cores. With --host-cores-dir we use it in
# place (the caller owns it); with --source-container we copy them to a temp dir.
cleanup_host_cores=false
if [[ -n "$HOST_CORES_DIR" ]]; then
  host_cores="$HOST_CORES_DIR"
else
  if ! docker inspect "$SOURCE_CONTAINER" >/dev/null 2>&1; then
    echo "Container '$SOURCE_CONTAINER' not found — nothing to collect."
    exit 0
  fi
  host_cores="$(mktemp -d)"
  cleanup_host_cores=true
  docker cp "${SOURCE_CONTAINER}:${CORES_DIR}/." "$host_cores" 2>/dev/null || true
fi

shopt -s nullglob
cores=("$host_cores"/core.*)
shopt -u nullglob
if [[ ${#cores[@]} -eq 0 ]]; then
  echo "No core dumps found in ${host_cores} — nothing to collect."
  [[ "$cleanup_host_cores" == true ]] && rm -rf "$host_cores"
  exit 0
fi
echo "Found ${#cores[@]} core dump(s) to analyze."

# Start a throwaway analyzer from the image, with the cores bind-mounted in at
# the expected path. sleep keeps it alive so collect.sh can docker exec it.
docker rm -f "$ANALYZER_CONTAINER" >/dev/null 2>&1 || true
if ! docker run -d --name "$ANALYZER_CONTAINER" --entrypoint sleep \
       -v "${host_cores}:${CORES_DIR}" "$IMAGE" infinity >/dev/null 2>&1; then
  echo "Warning: could not start analyzer container from ${IMAGE} (continuing)." >&2
  [[ "$cleanup_host_cores" == true ]] && rm -rf "$host_cores"
  exit 0
fi

# Delegate to collect.sh: analyze (gdb is in the debug image), upload the trace,
# the core and the binary, and ping monitoring. Run everything as root — the
# cores are root-owned and the image runs as the unprivileged memgraph user.
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

# collect.sh ran as root in the analyzer, so the cores + stack traces in the
# bind-mounted host dir are root-owned. Chown them back (via the still-running
# analyzer) so the host dir can be cleaned by the non-root caller.
docker exec -u root "$ANALYZER_CONTAINER" chown -R "$(id -u):$(id -g)" "$CORES_DIR" >/dev/null 2>&1 || true
docker rm -f "$ANALYZER_CONTAINER" >/dev/null 2>&1 || true
if [[ "$cleanup_host_cores" == true ]]; then
  rm -rf "$host_cores" 2>/dev/null || true
fi
