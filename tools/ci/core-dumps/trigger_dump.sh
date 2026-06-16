#!/usr/bin/env bash
# Validate the CI core-dump pipeline end to end.
#
# Runs INSIDE the build container (invoked by `mgbuild.sh trigger-dump`). It
# starts the freshly-built Memgraph and, in parallel, pushes a pathologically
# deep nested query that overflows the parser stack -> SIGSEGV -> core dump in
# /tmp/mg-cores. The surrounding job is expected to treat this as a failure and
# jump straight to the core-dump analysis steps.
#
# The query is generated with python3 (the same one-liner used to reproduce the
# crash by hand) and sent via mgconsole, which the build container always
# provides once the toolchain is activated.
set -uo pipefail

BINARY="/home/mg/memgraph/build/memgraph"
BOLT_PORT="7687"
CORES_DIR="/tmp/mg-cores"
DEPTH="200000"
TOOLCHAIN=""

print_usage() {
  cat <<EOF
Usage: trigger_dump.sh [OPTIONS]

Options:
  --binary PATH     Memgraph binary to run (default: $BINARY)
  --bolt-port PORT  Bolt port for the throwaway instance (default: $BOLT_PORT)
  --cores-dir DIR   Expected core dump directory (default: $CORES_DIR)
  --depth N         Nesting depth of the crash query (default: $DEPTH)
  --toolchain VER   Toolchain version to activate for python3 (e.g. v7)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)    BINARY="$2"; shift 2 ;;
    --bolt-port) BOLT_PORT="$2"; shift 2 ;;
    --cores-dir) CORES_DIR="$2"; shift 2 ;;
    --depth)     DEPTH="$2"; shift 2 ;;
    --toolchain) TOOLCHAIN="$2"; shift 2 ;;
    -h|--help)   print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ ! -x "$BINARY" ]]; then
  echo "Error: Memgraph binary '$BINARY' not found or not executable. Build it first." >&2
  exit 1
fi

# Activate the toolchain for python3. The activate script references zsh-only
# vars, which trips `set -u`; drop nounset just for the source.
if [[ -n "$TOOLCHAIN" && -f "/opt/toolchain-${TOOLCHAIN}/activate" ]]; then
  set +u
  # shellcheck disable=SC1090
  source "/opt/toolchain-${TOOLCHAIN}/activate"
  set -u
fi

mkdir -p "$CORES_DIR" 2>/dev/null || true
if ! ulimit -c unlimited 2>/dev/null; then
  echo "Warning: could not raise core size limit (ulimit -c); a core may not be written." >&2
fi

DATA_DIR="$(mktemp -d /tmp/td-data.XXXXXX)"
cleanup() { rm -rf "$DATA_DIR" /tmp/td-query.*.cypher 2>/dev/null || true; }
trap cleanup EXIT

echo "Starting Memgraph ($BINARY) on bolt port ${BOLT_PORT} ..."
"$BINARY" \
  --bolt-port="$BOLT_PORT" \
  --data-directory="$DATA_DIR" \
  --telemetry-enabled=false \
  --log-level=ERROR &
MG_PID=$!

echo "Waiting for Memgraph to accept Bolt connections ..."
ready=false
for _ in $(seq 1 60); do
  if ! kill -0 "$MG_PID" 2>/dev/null; then
    echo "Memgraph exited before becoming ready." >&2
    break
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/${BOLT_PORT}") 2>/dev/null; then
    ready=true
    echo "Bolt is up on ${BOLT_PORT}."
    break
  fi
  sleep 0.5
done
[[ "$ready" == true ]] || echo "Warning: proceeding without confirmed Bolt readiness." >&2

# Generate the crash query with the same python one-liner used to reproduce
# the stack overflow by hand.
QUERY_FILE="$(mktemp /tmp/td-query.XXXXXX.cypher)"
python3 -c "n=${DEPTH}; print('RETURN '+'('*n+'1'+')'*n+';')" > "$QUERY_FILE"
echo "Generated a ${DEPTH}-deep nested query; sending it to crash the parser ..."

if ! command -v mgconsole >/dev/null 2>&1; then
  echo "Error: mgconsole not found on PATH (expected in the build container after toolchain activation)." >&2
  exit 1
fi
echo "Sending query via mgconsole ..."
mgconsole --host 127.0.0.1 --port "$BOLT_PORT" < "$QUERY_FILE" || true

# Reap Memgraph (it should have been killed by the signal that produced the core).
wait "$MG_PID" 2>/dev/null
mg_status=$?
echo "Memgraph exited with status ${mg_status} (a signal kill is expected on crash)."

shopt -s nullglob
cores=("$CORES_DIR"/core.*)
shopt -u nullglob
if [[ ${#cores[@]} -gt 0 ]]; then
  echo "Core dump(s) produced in ${CORES_DIR}:"
  ls -lh "${cores[@]}"
  exit 0
fi

echo "WARNING: no core dump found in ${CORES_DIR}." >&2
echo "Check that kernel.core_pattern points at ${CORES_DIR} and that ulimit -c is unlimited." >&2
exit 2
