#!/usr/bin/env bash
# Produce human-readable gdb stack traces from Memgraph core dumps.
#
# This script is meant to run INSIDE the mgbuild test container (the Memgraph
# repo is copied to /home/mg/memgraph there, so this script ships with it).
# It walks a directory of core dumps (default /tmp/mg-cores), and for each core
# matching `core.*` it runs gdb against the Memgraph binary and writes a
# call-chain-only backtrace (`thread apply all bt`) to the output directory.
#
# gdb is provided by the toolchain (/opt/toolchain-<ver>/bin/gdb); pass
# --toolchain so we can activate it. If gdb is already on PATH that is used
# instead.
set -euo pipefail

CORES_DIR="/tmp/mg-cores"
BINARY="/home/mg/memgraph/build/memgraph"
OUT_DIR="/tmp/mg-cores/stacktraces"
TOOLCHAIN=""
CORE_GLOB="core.*"

print_usage() {
  cat <<EOF
Usage: analyze_core_dumps.sh [OPTIONS]

Options:
  --cores-dir DIR   Directory to scan for core dumps (default: $CORES_DIR)
  --binary PATH     Path to the Memgraph binary (default: $BINARY)
  --out-dir DIR     Directory to write stack traces to (default: $OUT_DIR)
  --toolchain VER   Toolchain version used to locate gdb (e.g. v7)
  --core-glob PAT   Glob (relative to --cores-dir) matching cores (default: $CORE_GLOB)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cores-dir) CORES_DIR="$2"; shift 2 ;;
    --binary)    BINARY="$2"; shift 2 ;;
    --out-dir)   OUT_DIR="$2"; shift 2 ;;
    --toolchain) TOOLCHAIN="$2"; shift 2 ;;
    --core-glob) CORE_GLOB="$2"; shift 2 ;;
    -h|--help)   print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

# Activate the toolchain so gdb is available, unless gdb is already on PATH.
if ! command -v gdb >/dev/null 2>&1 && [[ -n "$TOOLCHAIN" && -f "/opt/toolchain-${TOOLCHAIN}/activate" ]]; then
  # The toolchain activate script references zsh-only vars (e.g. $ZSH_NAME),
  # which trips our `set -u`. Drop nounset just for the source, then restore.
  set +u
  # shellcheck disable=SC1090
  source "/opt/toolchain-${TOOLCHAIN}/activate"
  set -u
fi

if ! command -v gdb >/dev/null 2>&1; then
  echo "Error: gdb not found (looked on PATH and in /opt/toolchain-${TOOLCHAIN:-<unset>})." >&2
  exit 1
fi

shopt -s nullglob
# shellcheck disable=SC2206 # CORE_GLOB is intentionally a glob pattern
cores=("$CORES_DIR"/$CORE_GLOB)
shopt -u nullglob

if [[ ${#cores[@]} -eq 0 ]]; then
  echo "No core dumps found in $CORES_DIR — nothing to analyze."
  exit 0
fi

if [[ ! -f "$BINARY" ]]; then
  echo "Warning: fallback binary '$BINARY' not found; any core that does not name its own executable will have NO SYMBOLS (addresses only)." >&2
fi

# A core records the path of every file mapped into the process, and the
# executable is the first of them, so a core from a test binary symbolises as
# well as one from memgraph, without the caller having to say which binary to
# expect. The command line the core also records is truncated to a fixed width
# by the kernel, so a binary sitting deep enough in the tree is unrecoverable
# from it. Falls back to the binary passed in when the core maps no file, or
# names one that is not present here.
resolve_binary_for_core() {
  local core="$1" exe
  # gdb exits non-zero on a core it cannot read, which would otherwise take the
  # whole run down and lose the cores not yet analysed.
  exe="$(gdb -batch -nx -ex "info proc mappings" --core="$core" 2>/dev/null |
    awk '$1 ~ /^0x/ { i = index($0, " /"); if (i > 0) { print substr($0, i + 1); exit } }' || true)"
  if [[ -n "$exe" && -f "$exe" ]]; then
    printf '%s' "$exe"
  else
    printf '%s' "$BINARY"
  fi
}

mkdir -p "$OUT_DIR"

count=0
for core in "${cores[@]}"; do
  base="$(basename "$core")"
  # Core files are named by kernel.core_pattern as `core.%t.%P.%s`
  # (epoch seconds . global PID . signal). When kernel.core_uses_pid=1 the kernel
  # also appends a trailing `.PID`, so tolerate an optional extra numeric field.
  # Derive a human-readable, URL-safe trace name from these fields, e.g.
  # stacktrace_2026-06-16T14-57-32Z_pid18330_sig11. Fall back to a sanitized core
  # name if the filename doesn't match the layout.
  epoch=""; pid=""; sig=""; trace_name=""
  if [[ "$base" =~ ^core\.([0-9]+)\.([0-9]+)\.([0-9]+)(\.[0-9]+)?$ ]]; then
    epoch="${BASH_REMATCH[1]}"
    pid="${BASH_REMATCH[2]}"
    sig="${BASH_REMATCH[3]}"
    when="$(date -u -d "@${epoch}" +%Y-%m-%dT%H-%M-%SZ 2>/dev/null || true)"
    [[ -n "$when" ]] && trace_name="stacktrace_${when}_pid${pid}_sig${sig}"
  fi
  [[ -z "$trace_name" ]] && trace_name="$(printf '%s' "$base" | tr -c 'A-Za-z0-9._-' '_')"
  out="$OUT_DIR/${trace_name}.txt"
  # Guard against two cores mapping to the same trace name (same second/pid/sig)
  # silently overwriting each other — append a counter if the name is taken.
  dup=1
  while [[ -e "$out" ]]; do
    out="$OUT_DIR/${trace_name}_${dup}.txt"
    dup=$((dup + 1))
  done
  core_binary="$(resolve_binary_for_core "$core")"
  echo "Analyzing $core ($(basename "$core_binary")) -> $out"
  {
    echo "=== Memgraph CI core dump stack trace ==="
    echo "core:      $core"
    [[ -n "$sig" ]] && echo "signal:    $sig"
    [[ -n "$epoch" ]] && echo "crashed:   $(date -u -d "@${epoch}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "epoch ${epoch}")"
    echo "binary:    $core_binary"
    [[ -f "$core_binary" ]] || echo "symbols:   MISSING — binary not found; backtrace shows addresses only, treat as unreliable"
    echo "generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "gdb:       $(gdb --version | head -n1)"
    echo "=========================================="
    echo
    # frame-arguments=none keeps the backtrace to functions + source locations
    # with no argument values, and plain `bt` (not `bt full`) omits locals — so
    # no crash-time memory is written into the uploaded trace.
    gdb -batch -nx \
      -ex "set pagination off" \
      -ex "set print frame-arguments none" \
      -ex "thread apply all bt" \
      -ex "info sharedlibrary" \
      -ex "quit" \
      "$core_binary" "$core" 2>&1 || echo "(gdb exited non-zero while analyzing $core)"
  } > "$out"
  count=$((count + 1))
done

echo "Produced $count stack trace(s) in $OUT_DIR"
