#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_multi_tenancy() {
  echo "FEATURE: Multi-tenancy basic check"
  run_next "SHOW DATABASES;"
}
