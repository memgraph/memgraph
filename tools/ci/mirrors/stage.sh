#!/usr/bin/env bash
# Copy the in-container mirror scripts into a docker build context.
#
#   tools/ci/mirrors/stage.sh <context-dir>
#
# leaves <context-dir>/mirrors/{pin_mirrors.sh,retry.sh}, which the Dockerfile
# picks up with a bind mount:
#
#   RUN --mount=type=bind,source=./mirrors,target=/mirrors,ro \
#     /mirrors/pin_mirrors.sh apply && \
#     /mirrors/retry.sh -- apt-get install -y ... && \
#     /mirrors/pin_mirrors.sh restore
#
# A build context is only ever the repo root or a directory the caller already
# stages packages into, so the staged copy is a transient artifact -- the
# contexts that live in the tree gitignore mirrors/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -ne 1 ]]; then
  echo "Usage: stage.sh <context-dir>" >&2
  exit 2
fi

context="$1"
if [[ ! -d "$context" ]]; then
  echo "Error: '$context' is not a directory" >&2
  exit 2
fi

mkdir -p "$context/mirrors"
cp "$SCRIPT_DIR/pin_mirrors.sh" "$SCRIPT_DIR/retry.sh" "$context/mirrors/"
chmod 0755 "$context/mirrors/pin_mirrors.sh" "$context/mirrors/retry.sh"
echo "Staged mirror scripts into $context/mirrors"
