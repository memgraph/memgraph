# Single source of truth for which bundled toolchain this repository builds
# against. Sourced by build.sh and by the tooling that has to locate the
# toolchain, so no script names a version inline.
#
# The value is a conan profile suffix: conan_config/profiles/memgraph_toolchain_<value>.
# A variant that shares an install prefix but changes settings is a value here
# too, so selecting one needs no new mechanism.
#
# Each profile knows its own install path, so build.sh never derives one. Where
# a script needs the path without going through conan it assumes the
# /opt/toolchain-<value> convention, which holds for plain version values; a
# variant whose prefix differs from its name needs MG_TOOLCHAIN_ROOT set.
#
# To build against a different toolchain, prefer overriding per invocation:
#   ./build.sh --toolchain v7
#   MG_TOOLCHAIN=v7 ./build.sh
#   MG_TOOLCHAIN_ROOT=/somewhere/else ./build.sh
# Edit this only when changing the default for everyone.
MG_TOOLCHAIN_DEFAULT=v8
