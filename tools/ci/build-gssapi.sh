#!/bin/bash

set -euo pipefail

# gssapi has no prebuilt Linux wheels on PyPI, so we have to build one
# from source against the libkrb5/krb5-devel headers available in the
# mgbuild container.

GSSAPI_VERSION="1.11.1"

# Build the wheel with the same interpreter memgraph embeds so its ABI tag
# matches where it gets installed. Most distros default python3 to that version,
# but CentOS Stream 9 ships 3.9 as python3 while memgraph is built against an
# explicitly-installed python3.12 — so prefer python3.12 when present (otherwise
# the wheel would be cp39 and rejected by the 3.12 install). Override with
# PYTHON=<interpreter> if needed. Mirrors release/package/mage/install_python_requirements.sh.
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  if command -v python3.12 >/dev/null 2>&1; then
    PYTHON=python3.12
  else
    PYTHON=python3
  fi
fi
echo "Building gssapi wheel with: $PYTHON ($($PYTHON --version 2>&1))"

rm -rf gssapi
mkdir -p gssapi/dist
cd gssapi
$PYTHON -m pip wheel --no-deps --no-cache-dir --no-binary=gssapi "gssapi==${GSSAPI_VERSION}" -w dist/
