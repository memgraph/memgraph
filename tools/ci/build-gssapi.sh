#!/bin/bash

set -euo pipefail

# gssapi has no prebuilt Linux wheels on PyPI, so we have to build one
# from source against the libkrb5/krb5-devel headers available in the
# mgbuild container.

GSSAPI_VERSION="1.9.0"

rm -rf gssapi
mkdir -p gssapi/dist
cd gssapi
pip3 wheel --no-deps --no-cache-dir --no-binary=gssapi "gssapi==${GSSAPI_VERSION}" -w dist/
