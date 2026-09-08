#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

if [ ! -x "$(command -v jq)" ]; then
 sudo apt-get install -y jq
fi

# Last released mgconsole.
# rm -rf $SCRIPT_DIR/bin # To download it again.
MG_CONSOLE_VERSION="v1.7.0"
MG_CONSOLE_BINARY="$SCRIPT_DIR/bin/mgconsole"
if [ ! -f "$MG_CONSOLE_BINARY" ]; then
  mkdir -p "$SCRIPT_DIR/bin"
  curl -fL "https://download.memgraph.com/mgconsole/$MG_CONSOLE_VERSION/linux-$(uname -m)/mgconsole" \
    -o "$MG_CONSOLE_BINARY"
  chmod +x "$MG_CONSOLE_BINARY"
fi
if [ -x "$MG_CONSOLE_BINARY" ]; then
  echo "$("$MG_CONSOLE_BINARY" --version) available at $MG_CONSOLE_BINARY"
else
  echo "failed to download mgconsole"
fi

cd $SCRIPT_DIR/query_modules
mkdir -p dist
g++ -std=c++20 -fPIC -shared -I$SCRIPT_DIR/../cpp/memgraph/include -o dist/basic_cpp.so basic.cpp
