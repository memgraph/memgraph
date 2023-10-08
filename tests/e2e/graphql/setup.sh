#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

source "$SCRIPT_DIR/../../util.sh"
setup_node

npm i
