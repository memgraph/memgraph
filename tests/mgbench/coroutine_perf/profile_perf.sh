#!/usr/bin/env bash
# Hardware-counter profiling of the coroutine pull path (P3.3; see ADRs/008_coroutine_cursors.md).
#
# Loads the chain graph, hammers a pull-intensive query in a tight loop, and captures perf HW
# counters + a sampled profile of the running memgraph. Run once flag-OFF and once flag-ON and
# compare. The KEY question this answers on bare metal: is the per-pull coroutine resume/suspend
# cost dominated by INDIRECT-BRANCH MISPREDICTS (which were the suspected nested-VM artifact)?
#
# REQUIRES real hardware PMU access (a bare-metal box, or a VM that exposes counters):
#   perf stat -e cycles,instructions must NOT report <not supported>.
# May need:  sudo sysctl kernel.perf_event_paranoid=1   (or -1)   and   kernel.kptr_restrict=0
#
# Usage:  ./profile_perf.sh <off|on> [/path/to/memgraph] [port]
#   env:  PYTHON=python3 (with mgclient), QUERY="MATCH (n:N)-[:R]->(m) RETURN count(*)", SECS=20
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:?usage: ./profile_perf.sh <off|on> [binary] [port]}"
MG="${2:-${MG_BINARY:-$HERE/../../../build/memgraph}}"
PORT="${3:-${PORT:-7799}}"
PY="${PYTHON:-python3}"
QUERY="${QUERY:-MATCH (n:N)-[:R]->(m) RETURN count(*)}"
SECS="${SECS:-20}"
FLAG=""; [[ "$MODE" == "on" ]] && FLAG="--experimental-enabled=coroutine-cursors"

rm -rf "/tmp/cperf_prof_$MODE"
# shellcheck disable=SC2086
"$MG" --bolt-port="$PORT" --data-directory="/tmp/cperf_prof_$MODE" --telemetry-enabled=false \
      --log-level=WARNING --storage-snapshot-on-exit=false $FLAG >"/tmp/cperf_prof_srv_$MODE.log" 2>&1 &
MGPID=$!
for _ in $(seq 1 60); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 1; done

# load the chain graph (idempotent) via the chain microbench's loader path
"$PY" "$HERE/p33_microbench.py" "$PORT" "prof_$MODE" >/dev/null 2>&1 || true

# background load
"$PY" "$HERE/hammer.py" "$PORT" "$QUERY" "$SECS" &
HAM=$!
sleep 1

echo "=== perf stat (HW counters), mode=$MODE, query: $QUERY ==="
perf stat -e cycles,instructions,branches,branch-misses,L1-dcache-load-misses \
  -p "$MGPID" -- sleep "$((SECS-3))" 2>&1 | sed -n '/Performance counter/,$p' || true

echo "=== sampled profile (flat self-time top-30) ==="
perf record -e task-clock -F 999 -p "$MGPID" -o "/tmp/cperf_$MODE.data" -- sleep 6 2>&1 | tail -1 || true
perf report -i "/tmp/cperf_$MODE.data" --stdio -g none --percent-limit 1 2>/dev/null | grep -vE '^#|^$' | head -30 || true

kill "$HAM" 2>/dev/null || true; kill "$MGPID" 2>/dev/null || true; wait "$MGPID" 2>/dev/null || true

cat <<'NOTE'

INTERPRET:
  * Compare flag-ON vs flag-OFF: cycles (the real overhead), IPC (instructions/cycles),
    and especially BRANCH-MISSES. If flag-ON branch-misses are much higher AND IPC drops,
    the per-pull coroutine dispatch (indirect-branch resume) is the cost -- and on bare metal
    with a good BTB it should be far smaller than the ~12-15% seen on the nested VM.
  * If flag-ON overhead is <=~5% on bare metal: the P3.3 gate passes; proceed to P3.4.
  * If still >5% with high branch-misses: the dispatch is a real cost -> revisit the
    per-pull dispatch design (e.g. fewer suspension points, batch resumes).
NOTE
