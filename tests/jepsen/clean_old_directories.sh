#! /bin/bash

MG_SNAPSHOT_DIR="/opt/memgraph/mg_data/snapshots"
MG_WAL_DIR="/opt/memgraph/mg_data/wal"
DATABASES_DIR="/opt/memgraph/mg_data/databases"
# Default value for JEPSEN_ACTIVE_NODES_NO
# shellcheck disable=SC2034
JEPSEN_ACTIVE_NODES_NO=0

# Function to delete .old* directories inside a given path
delete_old_dirs() {
  local folder_path="$1"
  echo "Cleaning .old directories inside $folder_path..."
  deleted_dirs=$(find "$folder_path" -maxdepth 1 -type d -name ".old*" -printf "%T@ %p\n" | sort -nr | awk 'NR > 1 {print $2}')
  if [[ -n "$deleted_dirs" ]]; then
    echo "Deleting the following directories:"
    echo "$deleted_dirs"
    echo "$deleted_dirs" | xargs -r rm -rf
  else
    echo "Nothing to delete in $folder_path. Only one or no .old* directory exists."
  fi
}

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

    # Clean snapshots folder
    docker exec "$jepsen_node_name" bash -c "
      $(declare -f delete_old_dirs)
      delete_old_dirs \"$MG_SNAPSHOT_DIR\"
      delete_old_dirs \"$MG_WAL_DIR\"
    "

    # Clean .old folders in all databases
    docker exec "$jepsen_node_name" bash -c "
      $(declare -f delete_old_dirs)
      DATABASES_DIR=\"$DATABASES_DIR\"
      for db_dir in \"\$DATABASES_DIR\"/*; do
        if [[ \$(basename \"\$db_dir\") != \"memgraph\" && -d \"\$db_dir\" ]]; then
          delete_old_dirs \"\$db_dir/snapshots\"
          delete_old_dirs \"\$db_dir/wal\"
        fi
      done
    "

    echo "[$(date)] Done with $jepsen_node_name"
  done

  sleep 300
done
