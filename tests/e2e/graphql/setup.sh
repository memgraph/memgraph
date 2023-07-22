#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

if ! command -v node >/dev/null; then
  echo "Could NOT node. Make sure node is installed."
  exit 1
fi
if ! command -v npm >/dev/null; then
  echo "Could NOT npm. Make sure npm is installed."
  exit 1
fi

npm ci
