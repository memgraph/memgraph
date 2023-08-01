#!/bin/bash

setup_node() {
  if [ -f "$HOME/.nvm/nvm.sh" ]; then
    . "$HOME/.nvm/nvm.sh"
    nvm install 14
    nvm use 14
  fi

  if ! command -v node >/dev/null; then
    echo "Could NOT node. Make sure node is installed."
    exit 1
  fi
  if ! command -v npm >/dev/null; then
    echo "Could NOT npm. Make sure npm is installed."
    exit 1
  fi
  node_version=$(node --version)
  npm_version=$(npm --version)
  echo "NODE VERSION: $node_version"
  echo "NPM  VERSION: $npm_version"
  node_major_version=${node_version##v}
  node_major_version=${node_major_version%%.*}
  if [ ! "$node_major_version" -ge 14 ]; then
    echo "ERROR: It's required to have node >= 14."
    exit 1
  fi
}
