#!/bin/bash
# Runs every integration test suite concurrently, each on its own port block.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Well clear of memgraph's defaults (7687/7444/9091) and the fixed ports some
# tests use for their own helper servers (ldap 1389, license_info 5500, telemetry 9000).
PORT_BASE=30000
PORT_STRIDE=10

print_help() {
  echo -e "$0 [jobs] => run all under tests/integration in parallel (default jobs: nproc)"
  exit 1
}

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
cd "$DIR"
for name in *; do
  if [ ! -d "$name" ]; then continue; fi
  # Wait for a free slot.
  while [ "$(jobs -rp | wc -l)" -ge "$jobs" ]; do
    wait -n
  done
  base=$((PORT_BASE + index * PORT_STRIDE))
  MG_INTEGRATION_BOLT_PORT=$base \
  MG_INTEGRATION_MONITORING_PORT=$((base + 1)) \
  MG_INTEGRATION_METRICS_PORT=$((base + 2)) \
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
