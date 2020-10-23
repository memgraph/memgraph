#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# check if dotnet-sdk-2.1 is installed
for i in dotnet; do
  if ! which $i >/dev/null; then
    echo "Please install $i!"
    exit 1
  fi
done

for i in *; do
  if [ ! -d $i ]; then
    continue
  fi
  pushd $i
  dotnet publish -c release --self-contained --runtime linux-x64 --framework netcoreapp2.1 -o build/
  ./build/$i
  popd
done;
