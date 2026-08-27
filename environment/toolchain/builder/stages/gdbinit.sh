#!/bin/bash
# gdbinit: the system gdb init file, written into the toolchain.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/gdbinit.env"

pushd "$TC_BUILD"
log_tool_name "setup system gdbinit"
mkdir -p $PREFIX/etc/gdb
# Nothing in here may name the install path. gdb reaches this file by
# relocating its configured path against its own binary, so it is found under
# whatever prefix the toolchain was extracted to -- but the paths inside a file
# are text, and the pass that makes the tree relocatable rewrites runpaths in
# ELF files only. A path written here would keep pointing at the machine the
# toolchain was built on, and gdb would start normally with no pretty printers
# and no pahole.
#
# So they are derived instead, from gdb's own data directory, which sits at
# <prefix>/share/gdb and is itself relocated by gdb.
cat >$PREFIX/etc/gdb/gdbinit <<'EOF'
# improve formatting
set print pretty on
set print object on
set print static-members on
set print vtbl on
set print demangle on
set demangle-style gnu-v3
set print sevenbit-strings off

python
import os
import sys
import gdb

_prefix = os.path.dirname(os.path.dirname(gdb.parameter("data-directory")))

# libstdc++ pretty printers
gdb.execute("add-auto-load-scripts-directory " + os.path.join(_prefix, "lib64"))
gdb.execute("add-auto-load-safe-path " + _prefix)

# pahole
sys.path.insert(0, os.path.join(_prefix, "share", "pahole-gdb"))
import offsets
import pahole
end
EOF

popd
