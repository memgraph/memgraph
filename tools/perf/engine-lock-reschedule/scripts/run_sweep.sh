#!/usr/bin/env bash
# One-shot launcher for the full validation sweep on the perf box. Keeps the cached sudo credential
# fresh for the whole (multi-hour) run so ip netns/tc calls never stall on a password prompt, starts
# the thermal/throttle auditor alongside, then drives phase3_sweep.sh. Cleans up its helpers on exit.
#
# Run detached so it survives the shell (prime sudo once first so the -v below needs no tty):
#     sudo -v
#     nohup ./run_sweep.sh > sweep.out 2>&1 &
#
# Env passes straight through to the sweep: CANDIDATES, REPL_MS_LIST, DUR, REPS.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

sudo -v || { echo "FATAL: sudo credential needed (run 'sudo -v' first)"; exit 1; }
# refresh the sudo timestamp every 50s so it never lapses mid-sweep; dies if the cred is revoked
( while true; do sudo -n true 2>/dev/null || exit; sleep 50; done ) & KA=$!
# throttle auditor: logs bench-core freq + hottest zone throughout the run
"$HERE/thermal_watch.sh" 5 "$HERE/thermal_watch.log" >/dev/null 2>&1 & TW=$!
cleanup() { kill "$KA" "$TW" 2>/dev/null || true; }
trap cleanup EXIT

echo "=== sweep started $(date -u)  (keepalive=$KA thermal=$TW) ==="
"$HERE/phase3_sweep.sh"
echo "=== sweep complete $(date -u); throttle audit: $HERE/thermal_watch.log ==="
