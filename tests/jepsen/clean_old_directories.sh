#! /bin/bash

SNAPSHOT_DIR="/opt/memgraph/mg_data/snapshots"
# Default value for JEPSEN_ACTIVE_NODES_NO
# shellcheck disable=SC2034
JEPSEN_ACTIVE_NODES_NO=0

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes-no)
      JEPSEN_ACTIVE_NODES_NO="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "JEPSEN_ACTIVE_NODES_NO set to: $JEPSEN_ACTIVE_NODES_NO"

while true; do
  for iter in $(seq 1 "$JEPSEN_ACTIVE_NODES_NO"); do
    jepsen_node_name="jepsen-n$iter"

    echo "[$(date)] Deleting .old directories inside $jepsen_node_name..."

    docker exec "$jepsen_node_name" bash -c "
      SNAPSHOT_DIR=\"$SNAPSHOT_DIR\"
      deleted_dirs=\$(find \"\$SNAPSHOT_DIR\" -maxdepth 1 -type d -name \".old*\" -printf \"%T@ %p\n\" \
        | sort -nr \
        | awk 'NR > 1 {print \$2}')

      if [[ -n \"\$deleted_dirs\" ]]; then
        echo \"Deleting the following directories:\"
        echo \"\$deleted_dirs\"
        echo \"\$deleted_dirs\" | xargs -r rm -rf
      else
        echo \"Nothing to delete. Only one or no .old* directory exists.\"
      fi
    "

    echo "[$(date)] Done with $jepsen_node_name"
    done

  # Sleep 5min
  sleep 300
done;
