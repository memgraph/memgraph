#!/bin/bash
set -euo pipefail
cd $HOME

# Split the built query_modules into two tarballs so the prod docker stage
# never has to carry the .so.<N>.debug sidecars (deleting them in a later
# layer wouldn't reclaim the bytes from the layer that ran the extraction).
# The relwithdebinfo stage extracts mage-debug.tar.gz on top of the prod
# tree to recover sidecars next to their .so files.
tar -czvf $HOME/mage.tar.gz --exclude='*.debug' query_modules

# mage-debug.tar.gz must ALWAYS exist because the Dockerfile bind-mounts it
# unconditionally — type=bind doesn't support required=false (only type=secret
# does), so an absent file fails the build. When there are no sidecars
# (non-split-debug builds, or split-debug runs that didn't produce any
# .so.<N>.debug files), emit a near-empty tarball containing just the
# query_modules dir entry. tar refuses to create truly empty archives, so
# `--no-recursion query_modules` is the smallest valid payload — extracting
# it on top of the existing /usr/lib/memgraph/query_modules/ is a no-op.
if find query_modules -name '*.so*.debug' -print -quit | grep -q .; then
    find query_modules -name '*.so*.debug' -print0 | \
        tar -czvf $HOME/mage-debug.tar.gz --null -T -
else
    tar -czvf $HOME/mage-debug.tar.gz --no-recursion query_modules
fi
