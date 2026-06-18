#!/usr/bin/env bash
# Remove core dumps from a host directory that was bind-mounted into a memgraph
# container (e.g. the ha/docker stress deployment's /tmp/mg-cores).
#
# The cores are written by the in-container memgraph user, and core-dump
# analysis (gdb run as root) may add root-owned files, so the unprivileged CI
# runner often can't delete them from the sticky 1777 dir. We therefore delete
# what the runner can directly, then mop up the rest as root via a throwaway
# container reusing the (already-pulled) memgraph image.
#
# Best-effort: never fail the CI job.
set -uo pipefail

CORES_DIR="/tmp/mg-cores"
IMAGE=""

print_usage() {
  cat <<EOF
Usage: clean_host_cores.sh [--cores-dir DIR] [--image IMAGE]

Options:
  --cores-dir DIR  Host core-dump dir to clean (default: $CORES_DIR)
  --image IMAGE    Memgraph image used to delete root-owned files as root.
                   Optional; if omitted, only files the runner owns are removed.
  -h, --help       Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cores-dir) CORES_DIR="$2"; shift 2 ;;
    --image)     IMAGE="$2"; shift 2 ;;
    -h|--help)   print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$CORES_DIR" || "$CORES_DIR" == "/" ]]; then
  echo "Error: refusing to clean an empty or root cores dir" >&2
  exit 1
fi

if [[ ! -d "$CORES_DIR" ]]; then
  echo "Cores dir ${CORES_DIR} does not exist — nothing to clean."
  exit 0
fi

echo "Cleaning core dumps in ${CORES_DIR}..."

# Remove what the runner owns directly.
rm -rf "${CORES_DIR:?}"/* 2>/dev/null || true

# Mop up root/memgraph-user-owned files via a root container, if an image is
# available to start one from.
if [[ -n "$IMAGE" ]] && command -v docker >/dev/null 2>&1; then
  docker run --rm -u root --entrypoint sh \
    -v "${CORES_DIR}:${CORES_DIR}" "$IMAGE" \
    -c "rm -rf ${CORES_DIR:?}/* 2>/dev/null || true" 2>/dev/null || true
fi

remaining="$(find "$CORES_DIR" -mindepth 1 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$remaining" == "0" ]]; then
  echo "Core dump dir is now empty."
else
  echo "Warning: ${remaining} item(s) remain in ${CORES_DIR}." >&2
fi
