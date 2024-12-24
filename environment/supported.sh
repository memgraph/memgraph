#!/bin/bash -e

DEPRECATED=(
    debian-10 
    ubuntu-18
)

UNSUPPORTED=(
    fedora-41
)

SUPPORTED=(
    ubuntu-24.04
)

# TODO(gitbuda): Add all os versions.
echo "DEPRECATED : $DEPRECATED"
echo "UNSUPPORTED: $UNSUPPORTED"
echo "SUPPORTED  : $SUPPORTED"
