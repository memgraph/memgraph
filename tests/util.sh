#!/bin/bash

DISABLE_NODE=${DISABLE_NODE:-false}

# TODO(matt): rebuild mgbuild images with node 24 installed in mg user's directory
NODE_MIN_VERSION="${NODE_MIN_VERSION:-20}"
NODE_INSTALL_VERSION="${NODE_INSTALL_VERSION:-24.19.0}"

# True when a node on PATH is at least $1 major.
node_major_at_least() {
  local want="$1" have
  command -v node >/dev/null 2>&1 || return 1
  have="$(node --version 2>/dev/null)" || return 1
  have="${have##v}"
  have="${have%%.*}"
  [ -n "$have" ] || return 1
  [ "$have" -ge "$want" ] 2>/dev/null
}

setup_node() {
  if [ "$DISABLE_NODE" = "true" ]; then
    echo "Skipping node setup because DISABLE_NODE is set to true"
    return 0
  fi

  if [ -f "$HOME/.nvm/nvm.sh" ]; then
    . "$HOME/.nvm/nvm.sh"
    if ! node_major_at_least "$NODE_MIN_VERSION"; then
      nvm use default >/dev/null 2>&1 || true
    fi
    if ! node_major_at_least "$NODE_MIN_VERSION"; then
      echo "No node >= $NODE_MIN_VERSION installed under nvm; installing $NODE_INSTALL_VERSION"
      nvm install "$NODE_INSTALL_VERSION"
      nvm use "$NODE_INSTALL_VERSION"
    fi
  fi

  if ! command -v node >/dev/null; then
    echo "Could NOT find node. Make sure node is installed."
    exit 1
  fi

  echo "Activating pinned pnpm via corepack."
  corepack enable pnpm 2>/dev/null || corepack enable 2>/dev/null || true
  corepack prepare pnpm@10.33.4 --activate 2>/dev/null || true

  if ! command -v pnpm >/dev/null; then
    echo "Could NOT find pnpm. Make sure pnpm is installed."
    exit 1
  fi
  echo "NODE VERSION: $(node --version)"
  echo "PNPM VERSION: $(pnpm --version)"
  if ! node_major_at_least "$NODE_MIN_VERSION"; then
    echo "ERROR: It's required to have node >= $NODE_MIN_VERSION, found $(node --version)."
    exit 1
  fi
}
