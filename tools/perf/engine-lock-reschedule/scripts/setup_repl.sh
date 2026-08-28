#!/usr/bin/env bash
# Phase-3 topology: fast client link + a SEPARATE, slow replication link so a SYNC-replica COMMIT
# blocks on the (netem-delayed) replica ack while reads over the fast link stay quick.
#
#   client netns (10.0.0.1) --veth,netem CLIENT_MS-- main netns (10.0.0.2) --veth,netem REPL_MS-- replica netns (10.0.0.3)
#
# All isolated netns; does NOT touch host lo. Usage: setup_repl.sh <client_one_way_ms> <repl_one_way_ms>
set -euo pipefail
CLIENT_MS="${1:-0.5}"; REPL_MS="${2:-10}"

for ns in mgcli mgsrv mgrepl; do sudo ip netns del "$ns" 2>/dev/null || true; done
for ns in mgcli mgsrv mgrepl; do sudo ip netns add "$ns"; done

# client <-> main  (10.0.0.1 / 10.0.0.2)
sudo ip link add vcli netns mgcli type veth peer name vsrv netns mgsrv
sudo ip netns exec mgcli ip addr add 10.0.0.1/24 dev vcli
sudo ip netns exec mgsrv ip addr add 10.0.0.2/24 dev vsrv
# main <-> replica (10.0.1.2 / 10.0.1.3) on a second subnet
sudo ip link add vsrvr netns mgsrv type veth peer name vrepl netns mgrepl
sudo ip netns exec mgsrv ip addr add 10.0.1.2/24 dev vsrvr
sudo ip netns exec mgrepl ip addr add 10.0.1.3/24 dev vrepl

for pair in "mgcli vcli" "mgsrv vsrv" "mgsrv vsrvr" "mgrepl vrepl"; do
  set -- $pair; sudo ip netns exec "$1" ip link set "$2" up
done
for ns in mgcli mgsrv mgrepl; do sudo ip netns exec "$ns" ip link set lo up; done

# netem: fast client link, slow replication link (symmetric per direction; RTT ~= 2*delay)
if [ "$CLIENT_MS" != "0" ]; then
  sudo ip netns exec mgcli tc qdisc add dev vcli root netem delay "${CLIENT_MS}ms"
  sudo ip netns exec mgsrv tc qdisc add dev vsrv root netem delay "${CLIENT_MS}ms"
fi
if [ "$REPL_MS" != "0" ]; then
  sudo ip netns exec mgsrv   tc qdisc add dev vsrvr root netem delay "${REPL_MS}ms"
  sudo ip netns exec mgrepl  tc qdisc add dev vrepl root netem delay "${REPL_MS}ms"
fi

echo "=== client->main RTT (expect ~$(echo "2*$CLIENT_MS"|bc)ms) ==="
sudo ip netns exec mgcli ping -c 3 -q 10.0.0.2 | tail -2
echo "=== main->replica RTT (expect ~$(echo "2*$REPL_MS"|bc)ms) ==="
sudo ip netns exec mgsrv ping -c 3 -q 10.0.1.3 | tail -2
