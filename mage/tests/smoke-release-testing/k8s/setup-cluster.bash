#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

#### INPUTs
# Exact routing?

BOLT_SERVER="localhost:10000" # Just tmp value -> each coordinator should have a different value.
# E.g. if kubectl port-foward is used, the configured host values should be passed as `bolt_server`.
####

# NOTE: This script is flaky because it's unknown who will actually be the
# leader. How to know who will be the leader or ask who is the leader? -> N/A
# -> explained under
# https://memgraph.com/docs/clustering/high-availability#add-coordinator-instance

kubectl port-forward memgraph-coordinator-1-0 17687:7687 &
PF_PID=$!
sleep 2
echo "ADD COORDINATOR 1 WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\":  \"memgraph-coordinator-1.default.svc.cluster.local:10000\", \"coordinator_server\":  \"memgraph-coordinator-1.default.svc.cluster.local:12000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
echo "ADD COORDINATOR 2 WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\":  \"memgraph-coordinator-2.default.svc.cluster.local:10000\", \"coordinator_server\":  \"memgraph-coordinator-2.default.svc.cluster.local:12000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
echo "ADD COORDINATOR 3 WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\":  \"memgraph-coordinator-3.default.svc.cluster.local:10000\", \"coordinator_server\":  \"memgraph-coordinator-3.default.svc.cluster.local:12000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
echo "REGISTER INSTANCE instance_0 WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\": \"memgraph-data-0.default.svc.cluster.local:10000\", \"replication_server\": \"memgraph-data-0.default.svc.cluster.local:20000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
echo "REGISTER INSTANCE instance_1 WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\": \"memgraph-data-1.default.svc.cluster.local:10000\", \"replication_server\": \"memgraph-data-1.default.svc.cluster.local:20000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
echo "SET INSTANCE instance_1 TO MAIN;" | $MEMGRAPH_CONSOLE_BINARY --port 17687
kill $PF_PID
wait $PF_PID 2>/dev/null

# How to know who is the leader? -> SHOW INSTANCES; on any of the coordinator.

# How to automated? -> ???
#   * Headless service -> it's a different type of routing (more efficient). ->
#     it doesn't depend on the code above (the code above is the same).
#   * NodePort doesn't work under Azure.
#   * only viable option is to use whatever on the serviceType, and setup
#     BOLT_SERVER to the port-forward ports, obviously that has limitations, at
#     least you can access pods from all machines that have k8s access.
