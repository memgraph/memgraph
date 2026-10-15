#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

# Last released mgconsole.
# rm -rf $SCRIPT_DIR/bin # To download it again.
MG_CONSOLE_VERSION="v1.7.0"
MG_CONSOLE_BINARY="$SCRIPT_DIR/bin/mgconsole"
if [ ! -f "$MG_CONSOLE_BINARY" ]; then
  mkdir -p "$SCRIPT_DIR/bin"
  # TODO(matt): build the mgconsole release for GLIBC 2.31 and drop the
  # toolchain override. The released binary for version 1.7.0 is built
  # against GLIBC 2.38+, which fails on older smoke targets (e.g. debian-12
  # ships 2.36); the toolchain's mgconsole is built against the sysroot (GLIBC floor 2.31).
  # mgconsole 1.7.1 will be built against the new toolchain and this can be removed.
  toolchain_mgconsole=""
  if [ -n "${MG_TOOLCHAIN_ROOT:-}" ] && [ -x "$MG_TOOLCHAIN_ROOT/bin/mgconsole" ]; then
    toolchain_mgconsole="$MG_TOOLCHAIN_ROOT/bin/mgconsole"
  else
    toolchain_mgconsole="$(ls -1 /opt/toolchain-*/bin/mgconsole 2>/dev/null | sort -V | tail -1 || true)"
  fi
  if [ -x "$toolchain_mgconsole" ]; then
    echo "Using toolchain mgconsole: $toolchain_mgconsole"
    cp "$toolchain_mgconsole" "$MG_CONSOLE_BINARY"
  else
    curl -fL "https://download.memgraph.com/mgconsole/$MG_CONSOLE_VERSION/linux-$(uname -m)/mgconsole" \
      -o "$MG_CONSOLE_BINARY"
  fi
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
