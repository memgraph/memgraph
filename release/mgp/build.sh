#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$SCRIPT_DIR"

SOURCE_MGP="$REPO_ROOT/include/mgp.py"
TARGET_MGP="$SCRIPT_DIR/mgp.py"

if [[ ! -f "$SOURCE_MGP" ]]; then
  echo "Missing source file: $SOURCE_MGP"
  exit 1
fi

if [[ -L "$TARGET_MGP" ]]; then
  echo "Removing existing symlink: $TARGET_MGP"
  rm -f "$TARGET_MGP"
fi

echo "Copying $SOURCE_MGP -> $TARGET_MGP"
cp -f "$SOURCE_MGP" "$TARGET_MGP"

if ! command -v poetry >/dev/null 2>&1; then
  echo "poetry is not installed. Install it with: pip install poetry"
  exit 1
fi

echo "Building mgp package"
poetry build

echo "Package built successfully in $SCRIPT_DIR/dist"
