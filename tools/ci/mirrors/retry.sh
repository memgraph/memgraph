#!/usr/bin/env bash
# Run a command, retrying it with a growing wait between attempts.
#
# Written for package-manager and docker-build commands, where the failure we
# actually see in CI is a mirror mid-sync: the window closes on its own within
# a minute or two, so waiting and refetching the metadata is the whole fix.
# Pairs with pin_mirrors.sh -- pinning picks better mirrors, this rides out the
# ones that go bad anyway.
#
#   retry.sh -- apt-get install -y foo
#   retry.sh --attempts 5 --delay 30 -- docker build .
#
# The wait before attempt N is (N-1) * delay seconds, so the default of 5
# attempts at 10s spends at most 60s waiting.

set -euo pipefail

attempts=5
delay=10

print_help() {
  sed -n '2,17p' "$0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --attempts)
      [[ $# -ge 2 ]] || { echo "Error: --attempts requires a value" >&2; exit 2; }
      attempts="$2"; shift 2
    ;;
    --delay)
      [[ $# -ge 2 ]] || { echo "Error: --delay requires a value" >&2; exit 2; }
      delay="$2"; shift 2
    ;;
    -h|--help) print_help; exit 0 ;;
    --) shift; break ;;
    *)
      echo "Error: unknown argument '$1' (did you forget the -- before the command?)" >&2
      exit 2
    ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "Error: no command given" >&2
  print_help >&2
  exit 2
fi

attempt=1
while true; do
  # Capture the status with `|| status=$?`, not from an `if`: a failed `if`
  # condition leaves $? at 0, which would make this exit 0 after every attempt
  # had failed and hide the failure from the caller.
  status=0
  "$@" || status=$?
  if (( status == 0 )); then
    exit 0
  fi
  if (( attempt >= attempts )); then
    echo "retry: '$*' failed after $attempts attempts" >&2
    exit "$status"
  fi
  wait_for=$(( attempt * delay ))
  echo "retry: '$*' failed (attempt $attempt/$attempts), retrying in ${wait_for}s..." >&2
  sleep "$wait_for"
  attempt=$(( attempt + 1 ))
done
