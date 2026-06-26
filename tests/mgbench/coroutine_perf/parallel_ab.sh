#!/usr/bin/env bash
# Parallel-execution sanity A/B (perf-gate step 3.4): OFF/SPLIT/ALL on USING PARALLEL EXECUTION
# aggregate/order-by queries. PR-14 keeps the parallel region fully synchronous, so all three
# arms should be ~flat. Requires an enterprise license in the environment.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MG="${1:-build/memgraph}"
PORT="${2:-7796}"
: "${MEMGRAPH_ENTERPRISE_LICENSE:?set the enterprise license env first}"

wait_port(){ for _ in $(seq 1 60); do (echo >"/dev/tcp/127.0.0.1/$PORT")2>/dev/null&&return 0;sleep 0.5;done;return 1;}

run_one(){ # tag flag
  local tag=$1 flag=$2
  rm -rf "/tmp/par_data_$tag"
  # shellcheck disable=SC2086
  taskset -c 0-7 "$MG" --bolt-port="$PORT" --data-directory="/tmp/par_data_$tag" --telemetry-enabled=false \
     --log-level=WARNING --storage-snapshot-on-exit=false $flag >"/tmp/par_srv_$tag.log" 2>&1 &
  local pid=$!; wait_port || { echo "bind fail $tag"; cat "/tmp/par_srv_$tag.log"; exit 1; }
  taskset -c 0-7 python3 "$HERE/parallel_ab.py" "$PORT" "$tag"
  kill "$pid" 2>/dev/null||true; wait "$pid" 2>/dev/null||true; sleep 1
}

echo "binary: $MG ($("$MG" --version 2>/dev/null | grep -oiE '[0-9a-f]{9,}' | head -1))"
run_one off   ""
run_one all   "--query-coroutine-yield-ops=All"
run_one split "--query-coroutine-yield-ops=Aggregate,OrderBy"

python3 - <<'PY'
import json
o=json.load(open('/tmp/par_off.json')); a=json.load(open('/tmp/par_all.json')); s=json.load(open('/tmp/par_split.json'))
print(f"\n{'query':12s} {'OFF(ms)':>9s} {'ALL(ms)':>9s} {'ALL%':>7s} {'SPLIT(ms)':>10s} {'SPLIT%':>7s}")
for k in o:
    print(f"{k:12s} {o[k]:9.3f} {a[k]:9.3f} {100*(a[k]-o[k])/o[k]:+6.1f}% {s[k]:10.3f} {100*(s[k]-o[k])/o[k]:+6.1f}%")
PY
