#!/bin/bash

set -e

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $script_dir

if ! which nuget >/dev/null; then
    echo "Please install nuget!"
    exit 1
fi

if ! which mono >/dev/null; then
    echo "Please install mono!"
    exit 1
fi

if ! which mcs >/dev/null; then
    echo "Please install mcs!"
    exit 1
fi

driver=Neo4j.Driver
version=1.5.0

if [ ! -f $driver.dll ]; then
  nuget_dir=`mktemp -d nuget.XXXXXXXX` || exit 1
  nuget install $driver -Version $version -OutputDirectory $nuget_dir || exit 1
  cp $nuget_dir/$driver.$version/lib/net452/$driver.dll ./ || exit 1
  rm -rf $nuget_dir || exit 1
fi

mcs -reference:$driver.dll Basic.cs
mono Basic.exe

mcs -reference:$driver.dll Transactions.cs
mono Transactions.exe
