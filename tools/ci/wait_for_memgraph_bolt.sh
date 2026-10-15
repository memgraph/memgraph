#!/usr/bin/env bash

# Wait until Memgraph accepts Bolt connections via mgconsole.
# Usage: wait_for_memgraph_bolt <host> <port> [max_retries] [retry_delay_seconds]

wait_for_memgraph_bolt() {
  local host="${1:-127.0.0.1}"
  local port="${2:-7687}"
  local max_retries="${3:-${BOLT_MAX_RETRIES:-180}}"
  local retry_delay_seconds="${4:-${BOLT_RETRY_DELAY:-1}}"
  local attempt=1
  local started=$SECONDS
  local last_error=""

  while (( attempt <= max_retries )); do
    if last_error="$(echo "RETURN 1;" | mgconsole --host "${host}" --port "${port}" 2>&1)"; then
      echo "Memgraph became ready at ${host}:${port} after $(( SECONDS - started ))s (attempt ${attempt}/${max_retries})"
      return 0
    fi
    # A long silent wait tells whoever reads the log nothing, so mark progress.
    if (( attempt % 15 == 0 )); then
      echo "  still waiting for ${host}:${port} ($(( SECONDS - started ))s elapsed, attempt ${attempt}/${max_retries})"
    fi
    sleep "${retry_delay_seconds}"
    ((attempt++))
  done

  # Report the elapsed time, not just the attempt count: attempts say nothing
  # about how long we actually gave it once mgconsole itself is slow to fail.
  echo "Memgraph did not become ready at ${host}:${port} after ${max_retries} attempts ($(( SECONDS - started ))s)."
  if [[ -n "$last_error" ]]; then
    echo "Last mgconsole error: ${last_error}"
  fi
  return 1
}

# If executed directly, run with CLI args.
# If sourced, only define the function for caller use.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  wait_for_memgraph_bolt "$@"
  exit $?
fi
