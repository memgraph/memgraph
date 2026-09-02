#!/usr/bin/env bash
# Dry run: verify the two-netns "two-machine" sim AND the disjoint core-pinning are set up
# correctly, WITHOUT launching memgraph. Proves the harness before you spend a build+bench on it.
#
#   client netns (10.0.0.1, cores $CLI_CORES) --veth,netem-- main netns (10.0.0.2, cores $SRV_CORES) --veth,netem-- replica netns (10.0.1.3)
#
# Uses the SAME topology builder as the grid (setup_repl.sh), so a PASS here means the box is ready
# for phase3_grid.sh / phase3_run.sh. The single delay is applied to BOTH links for this sanity check.
#
# Usage:   ./dry_run.sh [one_way_delay_ms]        (default 10ms -> RTT ~= 20ms on both links)
# Override cores: SRV_CORES=0-7 CLI_CORES=8-11 ./dry_run.sh   (defaults match phase3_run.sh)
set -uo pipefail

DELAY_MS="${1:-10}"
SRV_CORES="${SRV_CORES:-0-7}"
CLI_CORES="${CLI_CORES:-8-11}"
FAIL=0
pass() { printf '  \033[32mOK\033[0m    %s\n' "$1"; return 0; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=1; return 0; }

cleanup() { for ns in mgcli mgsrv mgrepl; do sudo ip netns del "$ns" 2>/dev/null || true; done; }
trap cleanup EXIT

# Expand a taskset cpu-list ("0-5,8") into newline-separated ids (numeric-sorted).
expand() {
  local part a b i
  IFS=',' read -ra part <<<"$1"
  for p in "${part[@]}"; do
    if [[ "$p" == *-* ]]; then a=${p%-*}; b=${p#*-}; for ((i=a;i<=b;i++)); do echo "$i"; done
    else echo "$p"; fi
  done | sort -n
}

echo "=== 0. Preflight ==="
NPROC=$(nproc)
MAXID=$( { expand "$SRV_CORES"; expand "$CLI_CORES"; } | sort -n | tail -1)
if [ "$MAXID" -lt "$NPROC" ]; then pass "host has $NPROC cores; max pinned id $MAXID is online"
else fail "max pinned core id $MAXID >= nproc $NPROC (offline/absent core)"; fi
OVERLAP=$(comm -12 <(expand "$SRV_CORES" | sort) <(expand "$CLI_CORES" | sort) | tr '\n' ' ')
if [ -z "${OVERLAP// /}" ]; then pass "server [$SRV_CORES] and client [$CLI_CORES] core sets are disjoint"
else fail "server/client core sets OVERLAP on: $OVERLAP"; fi
if sudo -n true 2>/dev/null; then pass "passwordless sudo available"; else fail "sudo needs a password"; fi
for t in ip tc taskset ping; do command -v "$t" >/dev/null || fail "missing tool: $t"; done

echo "=== 1. Bring up the 3-netns client/main/replica sim (delay=${DELAY_MS}ms on both links) ==="
if "$(dirname "$0")/setup_repl.sh" "$DELAY_MS" "$DELAY_MS" >/tmp/netlab_setup.$$ 2>&1; then
  pass "setup_repl.sh created mgcli/mgsrv/mgrepl netns + veth + netem"
else fail "setup_repl.sh failed:"; sed 's/^/       /' /tmp/netlab_setup.$$; fi
rm -f /tmp/netlab_setup.$$

echo "=== 2. Link shaping (RTT vs expected ~$((2*DELAY_MS))ms on each link) ==="
LO=$(awk -v d="$DELAY_MS" 'BEGIN{print 2*d*0.8}'); HI=$(awk -v d="$DELAY_MS" 'BEGIN{print 2*d*1.5+1}')
check_rtt() {  # from_ns to_ip label
  local rtt
  rtt=$(sudo ip netns exec "$1" ping -c 5 -q "$2" 2>/dev/null | awk -F'/' '/rtt|round-trip/{print $5}')
  if [ -z "$rtt" ]; then fail "no RTT on $3 link ($1 could not ping $2)"; return; fi
  if awk -v r="$rtt" -v lo="$LO" -v hi="$HI" 'BEGIN{exit !(r>=lo && r<=hi)}'; then
    pass "$3 link RTT=${rtt}ms within [$LO, $HI]"
  else fail "$3 link RTT=${rtt}ms OUTSIDE expected [$LO, $HI]"; fi
}
check_rtt mgcli 10.0.0.2 "client->main"
check_rtt mgsrv 10.0.1.3 "main->replica"
check_qdisc() {  # ns dev label
  local q; q=$(sudo ip netns exec "$1" tc qdisc show dev "$2" 2>/dev/null | tr -d '\n')
  if echo "$q" | grep -q netem; then pass "netem qdisc on $3:${q#qdisc}"
  else fail "no netem qdisc on $3 ($1 dev $2)"; fi
}
check_qdisc mgsrv vsrv  "client-link egress"
check_qdisc mgsrv vsrvr "repl-link egress"

echo "=== 3. Core pinning (the piece under test) ==="
check_pin() {
  local ns="$1" cores="$2" want got gotids
  want=$(expand "$cores" | tr '\n' ' ')
  got=$(sudo ip netns exec "$ns" taskset -c "$cores" \
      bash -c 'grep -oP "Cpus_allowed_list:\s*\K.*" /proc/self/status' 2>/dev/null)
  gotids=$(expand "$got" | tr '\n' ' ')
  if [ "$gotids" = "$want" ]; then pass "$ns pinned to [$cores]  (affinity=$got)"
  else fail "$ns affinity=[$got] (ids '$gotids'), expected [$cores] (ids '$want')"; fi
}
check_pin mgsrv  "$SRV_CORES"
check_pin mgrepl "$SRV_CORES"
check_pin mgcli  "$CLI_CORES"

echo
if [ "$FAIL" = 0 ]; then echo "RESULT: PASS — harness set up correctly. Ready to run phase3_grid.sh / phase3_run.sh."
else echo "RESULT: FAIL — fix the above before benchmarking."; fi
exit "$FAIL"
