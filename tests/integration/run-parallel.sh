#!/bin/bash
# Runs every integration test suite concurrently, each on its own port block.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Well clear of memgraph's defaults (7687/7444/9091) and the fixed ports some
# tests use for their own helper servers (ldap 1389, license_info 5500, telemetry 9000).
PORT_BASE=30000
PORT_STRIDE=10

print_help() {
  echo -e "$0 [jobs]                    => run all under tests/integration in parallel (default jobs: nproc)"
  echo -e "$0 monitoring-targets <host> => print MEMGRAPH_METRICS_TARGETS/MEMGRAPH_LOG_WS_TARGETS for every suite"
  exit 1
}

# Suite i (sorted directory order) always gets the same block, so monitoring
# targets can be computed before anything runs.
list_suites() {
  cd "$DIR"
  for name in *; do
    if [ -d "$name" ]; then echo "$name"; fi
  done
}
bolt_port() { echo $((PORT_BASE + $1 * PORT_STRIDE)); }
monitoring_port() { echo $(( $(bolt_port "$1") + 1 )); }
metrics_port() { echo $(( $(bolt_port "$1") + 2 )); }

if [ "$1" = "monitoring-targets" ]; then
  host=$2
  if [ "$#" -ne 2 ] || [ -z "$host" ]; then
    print_help
  fi
  metrics_targets=()
  log_ws_targets=()
  index=0
  for name in $(list_suites); do
    metrics_targets+=("$host:$(metrics_port "$index")")
    log_ws_targets+=("$host:$(monitoring_port "$index")")
    index=$((index + 1))
  done
  echo "MEMGRAPH_METRICS_TARGETS=$(IFS=,; echo "${metrics_targets[*]}")"
  echo "MEMGRAPH_LOG_WS_TARGETS=$(IFS=,; echo "${log_ws_targets[*]}")"
  exit 0
fi

if [ "$#" -gt 1 ]; then
  print_help
fi
jobs=${1:-$(nproc)}
if ! [[ "$jobs" =~ ^[1-9][0-9]*$ ]]; then
  print_help
fi

log_dir=$(mktemp -d "${TMPDIR:-/tmp}/memgraph_integration_logs.XXXXXX")
echo "Running integration tests with $jobs parallel jobs (logs in $log_dir)"
echo

names=()
pids=()
index=0
for name in $(list_suites); do
  # Wait for a free slot.
  while [ "$(jobs -rp | wc -l)" -ge "$jobs" ]; do
    wait -n
  done
  MG_INTEGRATION_BOLT_PORT=$(bolt_port "$index") \
  MG_INTEGRATION_MONITORING_PORT=$(monitoring_port "$index") \
  MG_INTEGRATION_METRICS_PORT=$(metrics_port "$index") \
    "$DIR/run.sh" "$name" >"$log_dir/$name.log" 2>&1 &
  names+=("$name")
  pids+=($!)
  index=$((index + 1))
done

# Print each suite's log as it finishes so output isn't interleaved.
failed=()
for i in "${!pids[@]}"; do
  name=${names[$i]}
  if wait "${pids[$i]}"; then
    status="\033[1;32mPASSED\033[0m"
  else
    status="\033[1;31mFAILED\033[0m"
    failed+=("$name")
  fi
  echo -e "==================== $name: $status ===================="
  cat "$log_dir/$name.log"
  echo
done

echo "==================== Summary ===================="
echo "Ran ${#names[@]} suites, ${#failed[@]} failed."
if [ "${#failed[@]}" -gt 0 ]; then
  printf '  FAILED: %s\n' "${failed[@]}"
  exit 1
fi
