#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"


MG_CONSOLE_VERSION="v1.7.3"
MG_CONSOLE_BINARY="$SCRIPT_DIR/bin/mgconsole"
# Downloaded again whenever the binary there isn't the pinned version, so a
# version bump (or an older toolchain copy left in bin/) can't go unnoticed.
if [ ! -x "$MG_CONSOLE_BINARY" ] || \
   [[ "$("$MG_CONSOLE_BINARY" --version 2>/dev/null)" != *" version ${MG_CONSOLE_VERSION#v}" ]]; then
  mkdir -p "$SCRIPT_DIR/bin"
  curl -fL "https://download.memgraph.com/mgconsole/$MG_CONSOLE_VERSION/linux-$(uname -m)/mgconsole" \
    -o "$MG_CONSOLE_BINARY"
  chmod +x "$MG_CONSOLE_BINARY"
fi
if [ -x "$MG_CONSOLE_BINARY" ]; then
  echo "$("$MG_CONSOLE_BINARY" --version) available at $MG_CONSOLE_BINARY"
else
  echo "failed to obtain mgconsole"
  exit 1
fi

cd "$SCRIPT_DIR/query_modules"
mkdir -p dist
g++ -std=c++20 -fPIC -shared -I"$SCRIPT_DIR/../../include" -o dist/basic_cpp.so basic.cpp
