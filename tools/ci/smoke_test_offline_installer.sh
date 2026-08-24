#!/usr/bin/env bash

# Smoke test for the memgraph + MAGE offline installer (.run).
#
# Installs the .run inside a fresh ubuntu:24.04 container with no network
# access, starts memgraph, and verifies that:
#   1. it reaches Bolt readiness (via wait_for_memgraph_bolt.sh),
#   2. the enterprise license handed in through the environment is accepted,
#   3. every memgraph + MAGE query module actually loaded.
#
# The expected module counts are scanned out of the source tree here on the
# host — the container has neither the repo nor a network — and passed to the
# container as env vars. It is the same expectation the docker smoke test
# asserts (tests/smoke/features/query_modules.bash with IMAGE_TYPE=mage): an
# installed MAGE box exposes the memgraph built-ins plus every MAGE module.
#
# The script runs both halves of the test. Invoked normally it drives docker;
# it then mounts itself into the container and re-enters through --in-container
# to run the in-container half, which is plain bash because the fresh image has
# no python before the installer lays it down.
#
# Usage: smoke_test_offline_installer.sh <path-to-installer.run>
#        smoke_test_offline_installer.sh --in-container   (inside the container)
#
# Environment:
#   MEMGRAPH_ENTERPRISE_LICENSE / MEMGRAPH_ORGANIZATION_NAME
#     Forwarded to memgraph, which reads them at startup. When either is empty
#     memgraph runs community and the license assertion is skipped.
#   BASE_IMAGE   Image to install into (default ubuntu:24.04).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Absolute — docker -v rejects the relative path the caller may have used.
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BASE_IMAGE="${BASE_IMAGE:-ubuntu:24.04}"

MEMGRAPH_HOST=127.0.0.1
MEMGRAPH_PORT=7687
MEMGRAPH_LOG=/tmp/memgraph.log
BOLT_MAX_RETRIES=60
BOLT_RETRY_DELAY=1

# --------------------------------------------------------------------------
# In-container half.
# --------------------------------------------------------------------------

query_csv() {
  echo "$1" | mgconsole --host "$MEMGRAPH_HOST" --port "$MEMGRAPH_PORT" --output-format=csv
}

fail() {
  echo "Offline installer smoke test FAILED: $1" >&2
  echo "--- $MEMGRAPH_LOG ---" >&2
  cat "$MEMGRAPH_LOG" >&2 || true
  echo "--- end of $MEMGRAPH_LOG ---" >&2
  exit 1
}

# mgconsole csv output is a header line followed by the rows, with strings
# quoted; a single-value query therefore lands on line 2 once the quotes and
# any CR are stripped.
scalar_result() {
  query_csv "$1" | sed -n 2p | tr -d '"\r'
}

check_count() {
  local query="$1" expected="$2" label="$3" actual
  actual="$(scalar_result "$query")"
  if [[ "$actual" != "$expected" ]]; then
    fail "$label = $actual, expected $expected"
  fi
  echo "OK: $label = $actual"
}

check_license() {
  # Only the is_valid row is echoed — the full SHOW LICENSE INFO output
  # carries the license key itself.
  local license_row
  license_row="$(query_csv 'SHOW LICENSE INFO;' | grep is_valid || true)"
  echo "License: $license_row"
  case "$license_row" in
    *true*) ;;
    *) fail "enterprise license was not accepted" ;;
  esac
}

run_in_container() {
  local expected_procedures="${EXPECTED_PROCEDURES:?EXPECTED_PROCEDURES must be set}"
  local expected_functions="${EXPECTED_FUNCTIONS:?EXPECTED_FUNCTIONS must be set}"

  echo "==> Running the offline installer"
  /installer.run

  echo "==> Starting memgraph"
  # `su` without `-`: a login shell would wipe MEMGRAPH_ENTERPRISE_LICENSE and
  # MEMGRAPH_ORGANIZATION_NAME before memgraph gets a chance to read them.
  su memgraph -c "/usr/lib/memgraph/memgraph --also-log-to-stderr" > "$MEMGRAPH_LOG" 2>&1 &
  local mg_pid=$!
  # shellcheck disable=SC2064  # expand mg_pid now: it is a local, and the trap
  # fires once this function has already returned.
  trap "kill $mg_pid 2>/dev/null || true; wait $mg_pid 2>/dev/null || true" EXIT

  if ! bash /wait_for_memgraph_bolt.sh "$MEMGRAPH_HOST" "$MEMGRAPH_PORT" "$BOLT_MAX_RETRIES" "$BOLT_RETRY_DELAY"; then
    fail "memgraph never accepted Bolt connections"
  fi

  if [[ -n "${MEMGRAPH_ENTERPRISE_LICENSE:-}" ]]; then
    check_license
  else
    echo "No MEMGRAPH_ENTERPRISE_LICENSE set — skipping the license check."
  fi

  check_count 'CALL mg.procedures() YIELD * RETURN count(*) AS cnt;' "$expected_procedures" "loaded procedures"
  check_count 'CALL mg.functions() YIELD * RETURN count(*) AS cnt;' "$expected_functions" "loaded functions"

  echo "Offline installer smoke test PASSED."
}

# --------------------------------------------------------------------------
# Host half.
# --------------------------------------------------------------------------

expected_module_counts() {
  python3 "$REPO_ROOT/tools/ci/query_module_count/scan_query_modules.py" --target all --compact |
    python3 -c 'import json,sys; d=json.load(sys.stdin); total=lambda k: d["memgraph"]["counts"][k]["total"] + d["mage"]["counts"][k]["total"]; print(total("procedures"), total("functions"))'
}

run_on_host() {
  local installer="$1"
  if [[ ! -f "$installer" ]]; then
    echo "Error: offline installer not found at $installer" >&2
    exit 1
  fi
  installer="$(cd "$(dirname "$installer")" && pwd)/$(basename "$installer")"

  local counts expected_procedures expected_functions
  counts="$(expected_module_counts)"
  read -r expected_procedures expected_functions <<< "$counts"
  echo "Expecting $expected_procedures procedures and $expected_functions functions"

  # The license goes in as value-less -e flags so the key is taken from this
  # process' environment and never appears on the command line.
  docker run --rm \
    --network none \
    -e MEMGRAPH_ENTERPRISE_LICENSE \
    -e MEMGRAPH_ORGANIZATION_NAME \
    -e EXPECTED_PROCEDURES="$expected_procedures" \
    -e EXPECTED_FUNCTIONS="$expected_functions" \
    -v "$installer:/installer.run:ro" \
    -v "$SCRIPT_PATH:/smoke_test_offline_installer.sh:ro" \
    -v "$SCRIPT_DIR/wait_for_memgraph_bolt.sh:/wait_for_memgraph_bolt.sh:ro" \
    "$BASE_IMAGE" \
    bash /smoke_test_offline_installer.sh --in-container
}

main() {
  case "${1:-}" in
    --in-container)
      run_in_container
      ;;
    "")
      echo "Usage: $0 <path-to-installer.run>" >&2
      exit 1
      ;;
    -h | --help)
      echo "Usage: $0 <path-to-installer.run>"
      exit 0
      ;;
    *)
      run_on_host "$1"
      ;;
  esac
}

main "$@"
