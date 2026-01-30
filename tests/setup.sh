#!/bin/bash
# shellcheck disable=1091
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

DISABLE_NODE=${DISABLE_NODE:-false}
if [ "$DISABLE_NODE" = "true" ]; then
  echo "Skipping node setup because DISABLE_NODE is set to true"
  exit 0
fi
"$DIR"/e2e/graphql/setup.sh
