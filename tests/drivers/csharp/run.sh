#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

for i in mono mcs; do
    if ! which $i >/dev/null; then
        echo "Please install $i!"
        exit 1
    fi
done

DRIVER=Neo4j.Driver.dll

if [ ! -f $DRIVER ]; then
    driver_dir=$( mktemp -d driver.XXXXXXXX ) || exit 1
    cd $driver_dir || exit 1
    # Driver downloaded from: https://www.nuget.org/packages/Neo4j.Driver/1.5.3
    wget -nv https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/drivers/csharp/neo4j.driver.1.5.3.nupkg || exit 1
    unzip -q neo4j.driver.1.5.3.nupkg || exit 1
    cp lib/net452/Neo4j.Driver.dll .. || exit 1
    cd .. || exit 1
    rm -rf $driver_dir || exit 1
fi

mcs -reference:$DRIVER Basic.cs
mono Basic.exe

mcs -reference:$DRIVER Transactions.cs
mono Transactions.exe
