#!/bin/bash

set -euo pipefail

git clone https://github.com/memgraph/pymgclient.git  --recurse-submodules
cd pymgclient
git checkout v1.5.2
python3 setup.py bdist_wheel
