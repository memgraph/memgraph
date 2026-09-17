#!/bin/bash

DISABLE_NODE=${DISABLE_NODE:-false}

# MG_NODE_VERSION is the version mgbuild provisions for the mg user; read it from
# environment/util.sh rather than keeping a second copy that can drift from it.
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )/../environment" && pwd )/util.sh"
NODE_MIN_VERSION="${NODE_MIN_VERSION:-20}"
NODE_INSTALL_VERSION="${NODE_INSTALL_VERSION:-$MG_NODE_VERSION}"
PNPM_VERSION="${PNPM_VERSION:-10.33.4}"

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

  # pnpm is the only package manager these suites use and every project wants the
  # same pinned version, so install it straight from npm. This used to go through
  # corepack, which was worth it only while node bundled it: node >= 25 does not,
  # and nothing here uses the `packageManager` field corepack exists to read.
  # Under nvm `npm -g` writes into the active version's own prefix, so no sudo.
  if [ "$(pnpm --version 2>/dev/null)" != "$PNPM_VERSION" ]; then
    echo "Installing pnpm@$PNPM_VERSION."
    npm install -g "pnpm@$PNPM_VERSION" >/dev/null 2>&1 || true
    hash -r 2>/dev/null || true
  fi

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
