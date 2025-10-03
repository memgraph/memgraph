#!/bin/bash
# shellcheck disable=1091
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

"$DIR"/e2e/graphql/setup.sh
