#!/bin/bash
set -euo pipefail
cd $HOME

tar -czvf $HOME/mage.tar.gz --exclude='*.debug' query_modules

if find query_modules -name '*.so*.debug' -print -quit | grep -q .; then
    find query_modules -name '*.so*.debug' -print0 | \
        tar -czvf $HOME/mage-debug.tar.gz --null -T -
else
    tar -czvf $HOME/mage-debug.tar.gz --no-recursion query_modules
fi
