#!/usr/bin/env bash
# Analyze + upload Jepsen core dumps.
#
# run.sh's process-results gathers each node's cores into tests/jepsen/cores as
# jepsen-n<i>_<date>_core.<...>. The mgbuild container is still up and carries
# the toolchain gdb + the matching unstripped binary, so we copy the cores into
# the container and hand off to collect.sh, which produces stack traces, uploads
# the cores + binary, and pings monitoring — all into the shared ci-stack-traces/
# layout.
#
# This is the shared implementation behind the "Analyze Jepsen core dumps" step
# in diff_jepsen / reusable_stress_jepsen / reusable_release_tests, so the
# orchestration lives in one place instead of three copies of inline YAML.
#
# Best-effort: never fail the CI job.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

TOOLCHAIN="v7"
OS=""
RUN_ID=""
JOB_ID=""
CORES_DIR="tests/jepsen/cores"

print_usage() {
  cat <<EOF
Usage: collect_jepsen_cores.sh --os NAME --run-id ID --job-id ID [OPTIONS]

Options:
  --toolchain VER   Toolchain version (default: $TOOLCHAIN)
  --os NAME         Operating system (used to derive the mgbuild container name)
  --run-id ID       Workflow run id (required)
  --job-id ID       Job identifier (required)
  --cores-dir DIR   Host dir holding the gathered node cores (default: $CORES_DIR)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --toolchain) TOOLCHAIN="$2"; shift 2 ;;
    --os)        OS="$2"; shift 2 ;;
    --run-id)    RUN_ID="$2"; shift 2 ;;
    --job-id)    JOB_ID="$2"; shift 2 ;;
    --cores-dir) CORES_DIR="$2"; shift 2 ;;
    -h|--help)   print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$OS" || -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --os, --run-id and --job-id are required" >&2
  exit 1
fi

# Always clean up the host cores dir on exit, even on an early return.
cleanup_host() { rm -rf "$CORES_DIR" 2>/dev/null || true; }
trap cleanup_host EXIT

if [[ ! -d "$CORES_DIR" ]] || [[ -z "$(ls -A "$CORES_DIR" 2>/dev/null)" ]]; then
  echo "No core dumps collected in ${CORES_DIR} — skipping analysis."
  exit 0
fi

container="mgbuild_${TOOLCHAIN}_${OS}"
if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "Container '$container' not found — skipping Jepsen core analysis."
  exit 0
fi

# Move the gathered node cores into the container's core dir, then let collect.sh
# analyze + upload them. The cores are named '<node>_<date>_core.<...>', so the
# glob matches 'core.' anywhere in the name; they are root-owned after docker cp.
docker exec -u root "$container" mkdir -p /tmp/mg-cores
docker cp "$CORES_DIR/." "$container":/tmp/mg-cores/

"$SCRIPT_DIR/collect.sh" \
  --toolchain "$TOOLCHAIN" \
  --os "$OS" \
  --exec-user root \
  --core-glob '*core.*' \
  --upload-core true \
  --run-id "$RUN_ID" \
  --job-id "$JOB_ID" \
  || echo "Warning: collect.sh exited non-zero (continuing)." >&2
