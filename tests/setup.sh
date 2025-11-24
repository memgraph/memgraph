#!/bin/bash
# shellcheck disable=1091
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Temporarily disabled npm usage in CI
echo "NPM usage temporarily disabled in CI"
exit 0
"$DIR"/e2e/graphql/setup.sh
