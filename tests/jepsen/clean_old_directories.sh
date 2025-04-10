#! /bin/bash

MG_SNAPSHOT_DIR="/opt/memgraph/mg_data/snapshots"
DATABASES_DIR="/opt/memgraph/mg_data/databases"
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
  # Clean up in JEPSEN_ACTIVE_NODES_NO
  for iter in $(seq 1 "$JEPSEN_ACTIVE_NODES_NO"); do
    jepsen_node_name="jepsen-n$iter"

    echo "[$(date)] Deleting .old directories inside $jepsen_node_name..."

    # Clean snapshots folder in jepsen container
    docker exec "$jepsen_node_name" bash -c "
      MG_SNAPSHOT_DIR=\"$MG_SNAPSHOT_DIR\"
      deleted_dirs=\$(find \"\$MG_SNAPSHOT_DIR\" -maxdepth 1 -type d -name \".old*\" -printf \"%T@ %p\n\" \
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

    # Clean up the /opt/memgraph/mg_data/databases in the jepsen container
    echo "[$(date)] Cleaning .old directories inside $jepsen_node_name in databases..."

    docker exec "$jepsen_node_name" bash -c "
      DATABASES_DIR=\"$DATABASES_DIR\"
      for db_dir in \"\$DATABASES_DIR\"/*/; do
        # Skip the 'memgraph' directory
        if [[ \$(basename \"\$db_dir\") != \"memgraph\" && -d \"\$db_dir\" ]]; then
          echo \"Cleaning .old directories inside \$db_dir/snapshots...\"
          deleted_dirs=\$(find \"\$db_dir/snapshots\" -maxdepth 1 -type d -name \".old*\" -printf \"%T@ %p\n\" | sort -nr | awk 'NR > 1 {print \$2}')
          if [[ -n \"\$deleted_dirs\" ]]; then
            echo \"Deleting the following directories:\"
            echo \"\$deleted_dirs\"
            echo \"\$deleted_dirs\" | xargs -r rm -rf
          else
            echo \"Nothing to delete in \$db_dir/snapshots. Only one or no .old* directory exists.\"
          fi
        fi
      done
    "

    echo "[$(date)] Done with $jepsen_node_name"
  done

  # Sleep 5 minutes
  sleep 300
done;
