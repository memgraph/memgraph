#!/bin/bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $script_dir

output_dir=snapshots

if [ -d $output_dir ]; then
    rm -rf $output_dir
fi

NUM_MACHINES="$( cat card_fraud.py | grep -m1 "NUM_MACHINES" | tail -c 2 )"

build_dir=../../../build_release
if [ ! -d $build_dir ]; then
    build_dir=../../../build
fi
$build_dir/tests/manual/card_fraud_generate_snapshot --config config.json --num-workers $NUM_MACHINES --dir $output_dir
