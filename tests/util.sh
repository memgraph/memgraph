#!/bin/bash

DISABLE_NODE=${DISABLE_NODE:-false}

NODE_VERSION="20"
setup_node() {
  if [ "$DISABLE_NODE" = "true" ]; then
    echo "Skipping node setup because DISABLE_NODE is set to true"
    return 0
  fi
  if [ -f "$HOME/.nvm/nvm.sh" ]; then
    . "$HOME/.nvm/nvm.sh"
    nvm install $NODE_VERSION
    nvm use $NODE_VERSION
  fi

  if ! command -v node >/dev/null; then
    echo "Could NOT node. Make sure node is installed."
    exit 1
  fi

  if ! command -v pnpm >/dev/null; then
    echo "pnpm not found, enabling via corepack."
    corepack enable pnpm 2>/dev/null || corepack enable 2>/dev/null || true
  fi

  if ! command -v pnpm >/dev/null; then
    echo "Could NOT find pnpm. Make sure pnpm is installed."
    exit 1
  fi
  node_version=$(node --version)
  pnpm_version=$(pnpm --version)
  echo "NODE VERSION: $node_version"
  echo "PNPM VERSION: $pnpm_version"
  node_major_version=${node_version##v}
  node_major_version=${node_major_version%%.*}
  if [ ! "$node_major_version" -ge $NODE_VERSION ]; then
    echo "ERROR: It's required to have node >= $NODE_VERSION."
    exit 1
  fi
}
