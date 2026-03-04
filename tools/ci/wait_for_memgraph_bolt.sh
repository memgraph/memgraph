#!/usr/bin/env bash

# Wait until Memgraph accepts Bolt connections via mgconsole.
# Usage: wait_for_memgraph_bolt <host> <port> [max_retries] [retry_delay_seconds]
wait_for_memgraph_bolt() {
  local host="${1:-127.0.0.1}"
  local port="${2:-7687}"
  local max_retries="${3:-20}"
  local retry_delay_seconds="${4:-1}"
  local attempt=1

  while (( attempt <= max_retries )); do
    if echo "RETURN 1;" | mgconsole --host "${host}" --port "${port}" >/dev/null 2>&1; then
      echo "Memgraph became ready at ${host}:${port}"
      return 0
    fi
    sleep "${retry_delay_seconds}"
    ((attempt++))
  done

  echo "Memgraph did not become ready at ${host}:${port} after ${max_retries} attempts."
  return 1
}
