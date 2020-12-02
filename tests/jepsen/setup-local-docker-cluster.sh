#!/bin/bash

set -Eeuo pipefail
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# NOTE: docker and docker-compose have to be installed on the system.

if [ ! -d "$script_dir/jepsen" ]; then
    git clone https://github.com/jepsen-io/jepsen.git -b "0.2.1" "$script_dir/jepsen"
fi

"$script_dir/jepsen/docker/bin/up"
