# syntax=docker/dockerfile:1.7
#
# Toolchain build, one stage per tool.
#
# Each stage copies only its own recipe and its own version file, so editing
# the LLVM recipe or bumping the LLVM version rebuilds LLVM and what follows it
# and leaves GCC alone. Copying the repository in, or sharing one version file,
# would defeat that entirely: a change to any source file would rebuild the
# compiler.
#
# The stages are chained rather than fanned out. The bootstrap genuinely is a
# chain -- glibc needs the kernel headers, GCC needs glibc, everything after
# needs GCC -- and the recipes reach across the shared build directory in
# places (gmp and mpfr build from GCC's source tree, LLVM runs the swig that
# swig built). Fanning out the independent leaves is worth doing once the port
# is proven to reproduce v8, not before.
#
# Build with the justfile rather than by hand; it passes the arguments below.

ARG BASE_IMAGE=ubuntu@sha256:8feb4d8ca5354def3d8fce243717141ce31e2c428701f6682bd2fafe15388214

FROM ${BASE_IMAGE} AS base
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LANG=C.UTF-8 \
    LC_ALL=C

# Mirrors TOOLCHAIN_BUILD_DEPS in environment/os/ubuntu-20.04.sh.
#
# The base image is pinned by digest, but apt still resolves against the live
# archive, so the package versions here can move even when the image does not.
# That is the one remaining source of drift in the build environment, and it is
# what a snapshot archive would close; until then a rebuild months apart can
# pick up different builder packages.
COPY apt-packages.txt /tc/apt-packages.txt
RUN apt-get update && \
    xargs -a /tc/apt-packages.txt apt-get install -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# $DIR in the recipes: holds archives/, build/, and the version's own
# activate.in and toolchain.cmake.
WORKDIR /work
COPY files/ /work/
COPY lib/ /tc/lib/

# Cross-target and the builder's glibc, both build arguments so they are part
# of every stage's cache key rather than detected at run time.
ARG TC_FOR_ARM=false
ARG TC_DISTRO=ubuntu-20.04
ARG TC_BUILDER_GLIBC_FLOOR=2.31
ENV TC_FOR_ARM=${TC_FOR_ARM} \
    TC_DISTRO=${TC_DISTRO} \
    TC_BUILDER_GLIBC_FLOOR=${TC_BUILDER_GLIBC_FLOOR} \
    SOURCE_DATE_EPOCH=0

COPY versions/toolchain.env /tc/versions/toolchain.env

# --------------------------------------------------------------------------
# Bootstrap: kernel headers -> glibc -> GCC -> binutils.
# --------------------------------------------------------------------------

FROM base AS s-linux-headers
COPY versions/linux-headers.env /tc/versions/
COPY stages/linux-headers.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/linux-headers.sh

FROM s-linux-headers AS s-glibc
COPY versions/glibc.env /tc/versions/
COPY stages/glibc.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/glibc.sh

FROM s-glibc AS s-gcc
COPY versions/gcc.env /tc/versions/
COPY stages/gcc.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/gcc.sh

FROM s-gcc AS s-gcc-sysroot-libs
COPY versions/gcc-sysroot-libs.env /tc/versions/
COPY stages/gcc-sysroot-libs.sh /tc/stages/
RUN /tc/stages/gcc-sysroot-libs.sh

FROM s-gcc-sysroot-libs AS s-gmp
COPY versions/gmp.env /tc/versions/
COPY stages/gmp.sh /tc/stages/
RUN /tc/stages/gmp.sh

FROM s-gmp AS s-mpfr
COPY versions/mpfr.env /tc/versions/
COPY stages/mpfr.sh /tc/stages/
RUN /tc/stages/mpfr.sh

FROM s-mpfr AS s-binutils
COPY versions/binutils.env /tc/versions/
COPY stages/binutils.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/binutils.sh

# --------------------------------------------------------------------------
# Sysroot support libraries, built with the toolchain GCC so they target the
# sysroot's glibc, and installed into $SYSROOT/usr where that GCC finds them.
# Consumed by GDB, cmake and mgconsole.
# --------------------------------------------------------------------------

FROM s-binutils AS s-zlib
COPY versions/zlib.env /tc/versions/
COPY stages/zlib.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/zlib.sh

FROM s-zlib AS s-ncurses
COPY versions/ncurses.env /tc/versions/
COPY stages/ncurses.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/ncurses.sh

FROM s-ncurses AS s-openssl
COPY versions/openssl.env /tc/versions/
COPY stages/openssl.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/openssl.sh

FROM s-openssl AS s-curl
COPY versions/curl.env /tc/versions/
COPY stages/curl.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/curl.sh

FROM s-curl AS s-libffi
COPY versions/libffi.env /tc/versions/
COPY stages/libffi.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libffi.sh

FROM s-libffi AS s-python
COPY versions/python.env /tc/versions/
COPY stages/python.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/python.sh

# --------------------------------------------------------------------------
# Tools.
# --------------------------------------------------------------------------

FROM s-python AS s-cmake
COPY versions/cmake.env /tc/versions/
COPY stages/cmake.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/cmake.sh

FROM s-cmake AS s-libipt
COPY versions/libipt.env /tc/versions/
COPY stages/libipt.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libipt.sh

FROM s-libipt AS s-gdb
COPY versions/gdb.env versions/python.env /tc/versions/
COPY stages/gdb.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/gdb.sh

FROM s-gdb AS s-pahole
COPY versions/pahole.env /tc/versions/
COPY stages/pahole.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/pahole.sh

FROM s-pahole AS s-gdbinit
COPY versions/gdbinit.env /tc/versions/
COPY stages/gdbinit.sh /tc/stages/
RUN /tc/stages/gdbinit.sh

FROM s-gdbinit AS s-cppcheck
COPY versions/cppcheck.env /tc/versions/
COPY stages/cppcheck.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/cppcheck.sh

FROM s-cppcheck AS s-swig
COPY versions/swig.env /tc/versions/
COPY stages/swig.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/swig.sh

FROM s-swig AS s-llvm
COPY versions/llvm.env /tc/versions/
COPY stages/llvm.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/llvm.sh

# --------------------------------------------------------------------------
# Built with the finished toolchain rather than bootstrapping it.
# --------------------------------------------------------------------------

FROM s-llvm AS s-mgconsole
COPY versions/mgconsole.env /tc/versions/
COPY lib/clang-env.sh /tc/lib/
COPY stages/mgconsole.sh /tc/stages/
RUN /tc/stages/mgconsole.sh

FROM s-mgconsole AS s-heaptrack
COPY versions/heaptrack.env /tc/versions/
COPY stages/heaptrack.sh /tc/stages/
RUN /tc/stages/heaptrack.sh

# --------------------------------------------------------------------------
# Check, then package. The check alters nothing it inspects; it runs before
# packaging so a toolchain that breaks its own portability promise never
# reaches an archive.
# --------------------------------------------------------------------------

FROM s-heaptrack AS s-verify
COPY verify/ /tc/verify/
RUN /tc/verify/glibc-floor.sh

FROM s-verify AS s-package
COPY stages/package.sh /tc/stages/
RUN /tc/stages/package.sh

# Nothing runs to extract this: `--output type=local` writes it straight out.
FROM scratch AS artifact
COPY --from=s-package /work/output/ /
