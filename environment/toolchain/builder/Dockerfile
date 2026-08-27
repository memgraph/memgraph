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
# swig built). Fanning out the independent leaves is worth doing once this is
# proven against a toolchain we already trust, not before.
#
# Build with the justfile rather than by hand; it passes the arguments below.

# Supplied by the justfile from the version set's builder.env. No default:
# a default here would be a second place to change the base image, and the
# one that silently wins when the set is not consulted.
ARG BASE_IMAGE

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
COPY resolved/packages.txt /tc/packages.txt
RUN apt-get update && \
    grep -vE '^\s*(#|$)' /tc/packages.txt | xargs apt-get install -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# $DIR in the recipes: holds archives/, build/, and the version's own
# activate.in and toolchain.cmake.
WORKDIR /work
COPY files/ /work/
COPY lib/ /tc/lib/

# Build arguments so they are part of every stage's cache key rather than
# detected at run time. TC_FOR_ARM selects the cross-target and is a knob;
# the other two describe the build environment and come from the version set's
# builder.env, so they carry no default for the same reason BASE_IMAGE does
# not.
ARG TC_FOR_ARM=false
ARG TC_DISTRO
ARG TC_BUILDER_GLIBC_FLOOR
ENV TC_FOR_ARM=${TC_FOR_ARM} \
    TC_DISTRO=${TC_DISTRO} \
    TC_BUILDER_GLIBC_FLOOR=${TC_BUILDER_GLIBC_FLOOR} \
    SOURCE_DATE_EPOCH=0

# --------------------------------------------------------------------------
# Bootstrap: kernel headers -> glibc -> GCC -> binutils.
# --------------------------------------------------------------------------

FROM base AS s-linux-headers
COPY resolved/linux-headers.env /tc/versions/
COPY stages/linux-headers.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/linux-headers.sh

FROM s-linux-headers AS s-glibc
COPY resolved/glibc.env /tc/versions/
COPY stages/glibc.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/glibc.sh

FROM s-glibc AS s-gcc
COPY resolved/gcc.env /tc/versions/
COPY stages/gcc.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/gcc.sh

FROM s-gcc AS s-gcc-sysroot-libs
COPY resolved/gcc-sysroot-libs.env /tc/versions/
COPY stages/gcc-sysroot-libs.sh /tc/stages/
RUN /tc/stages/gcc-sysroot-libs.sh

FROM s-gcc-sysroot-libs AS s-gmp
COPY resolved/gmp.env /tc/versions/
COPY stages/gmp.sh /tc/stages/
RUN /tc/stages/gmp.sh

FROM s-gmp AS s-mpfr
COPY resolved/mpfr.env /tc/versions/
COPY stages/mpfr.sh /tc/stages/
RUN /tc/stages/mpfr.sh

FROM s-mpfr AS s-binutils
COPY resolved/binutils.env /tc/versions/
COPY stages/binutils.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/binutils.sh

# --------------------------------------------------------------------------
# Sysroot support libraries, built with the toolchain GCC so they target the
# sysroot's glibc, and installed into $SYSROOT/usr where that GCC finds them.
#
# What belongs in the sysroot, and what does not:
#
#   yes  a shared library a shipped binary loads at run time. gdb needs
#        libpython and libncurses, libabigail needs libxml2 and libdw. These
#        have to target the sysroot's glibc, because they are loaded next to
#        binaries that do.
#   yes  anything memgraph itself links against, which is what --sysroot points
#        its build at. In practice the C library, and the C++ runtime beside it
#        in lib64.
#   no   a library only needed to build one of these tools, where the result is
#        linked statically. That belongs in the set's packages.txt: it is a property
#        of the machine doing the building, never of the toolchain. boost and
#        elfutils are the examples -- heaptrack compiles against the builder's
#        copies and links them in, so neither ever reaches the sysroot.
#
# The one deliberate exception is zstd, which is static and build-time only but
# still installed here: LLVM is configured with FIND_ROOT_PATH_MODE_LIBRARY=ONLY
# so that nothing host-linked can slip into the compiler, and that setting also
# means the only place its build can find a library is the sysroot.
# --------------------------------------------------------------------------

FROM s-binutils AS s-zlib
COPY resolved/zlib.env /tc/versions/
COPY stages/zlib.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/zlib.sh

FROM s-zlib AS s-ncurses
COPY resolved/ncurses.env /tc/versions/
COPY stages/ncurses.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/ncurses.sh

FROM s-ncurses AS s-openssl
COPY resolved/openssl.env /tc/versions/
COPY stages/openssl.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/openssl.sh

FROM s-openssl AS s-curl
COPY resolved/curl.env /tc/versions/
COPY stages/curl.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/curl.sh

FROM s-curl AS s-libffi
COPY resolved/libffi.env /tc/versions/
COPY stages/libffi.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libffi.sh

# elfutils and libxml2 are here for dwz and libabigail below. They sit before
# LLVM on purpose: neither depends on it, and putting them after would mean an
# LLVM bump rebuilt them for nothing.
FROM s-libffi AS s-elfutils
COPY resolved/elfutils.env /tc/versions/
COPY stages/elfutils.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/elfutils.sh

FROM s-elfutils AS s-libxml2
COPY resolved/libxml2.env /tc/versions/
COPY stages/libxml2.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libxml2.sh

FROM s-libxml2 AS s-xxhash
COPY resolved/xxhash.env /tc/versions/
COPY stages/xxhash.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/xxhash.sh

FROM s-xxhash AS s-zstd
COPY resolved/zstd.env /tc/versions/
COPY stages/zstd.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/zstd.sh

FROM s-zstd AS s-xz
COPY resolved/xz.env /tc/versions/
COPY stages/xz.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/xz.sh

FROM s-xz AS s-dwz
COPY resolved/dwz.env /tc/versions/
COPY stages/dwz.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/dwz.sh

FROM s-dwz AS s-libabigail
COPY resolved/libabigail.env /tc/versions/
COPY stages/libabigail.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libabigail.sh

FROM s-libabigail AS s-python
COPY resolved/python.env /tc/versions/
COPY stages/python.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/python.sh

# --------------------------------------------------------------------------
# Tools.
# --------------------------------------------------------------------------

FROM s-python AS s-cmake
COPY resolved/cmake.env /tc/versions/
COPY stages/cmake.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/cmake.sh

FROM s-cmake AS s-mold
COPY resolved/mold.env /tc/versions/
COPY stages/mold.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/mold.sh

FROM s-mold AS s-libipt
COPY resolved/libipt.env /tc/versions/
COPY stages/libipt.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/libipt.sh

FROM s-libipt AS s-gdb
COPY resolved/gdb.env resolved/python.env /tc/versions/
COPY stages/gdb.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/gdb.sh

FROM s-gdb AS s-pahole
COPY resolved/pahole.env /tc/versions/
COPY stages/pahole.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/pahole.sh

FROM s-pahole AS s-gdbinit
COPY resolved/gdbinit.env /tc/versions/
COPY stages/gdbinit.sh /tc/stages/
RUN /tc/stages/gdbinit.sh

FROM s-gdbinit AS s-cppcheck
COPY resolved/cppcheck.env /tc/versions/
COPY stages/cppcheck.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/cppcheck.sh

FROM s-cppcheck AS s-swig
COPY resolved/swig.env /tc/versions/
COPY stages/swig.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/swig.sh

FROM s-swig AS s-llvm
COPY resolved/llvm.env /tc/versions/
COPY stages/llvm.sh /tc/stages/
RUN --mount=type=cache,target=/work/archives,sharing=locked /tc/stages/llvm.sh

# --------------------------------------------------------------------------
# Built with the finished toolchain rather than bootstrapping it.
# --------------------------------------------------------------------------

# The activation script has to exist before the stages below, which build
# against the finished toolchain and source it through clang-env.sh. In the
# original script that ordering was implicit in the line order.
FROM s-llvm AS s-activate
COPY resolved/activate.env /tc/versions/
COPY stages/activate.sh /tc/stages/
RUN /tc/stages/activate.sh

FROM s-activate AS s-mgconsole
COPY resolved/mgconsole.env /tc/versions/
COPY lib/clang-env.sh /tc/lib/
COPY stages/mgconsole.sh /tc/stages/
RUN /tc/stages/mgconsole.sh

FROM s-mgconsole AS s-heaptrack
COPY resolved/heaptrack.env /tc/versions/
COPY stages/heaptrack.sh /tc/stages/
RUN /tc/stages/heaptrack.sh

# --------------------------------------------------------------------------
# Check, then package. The check alters nothing it inspects; it runs before
# packaging so a toolchain that breaks its own portability promise never
# reaches an archive.
# --------------------------------------------------------------------------

FROM s-heaptrack AS s-verify
COPY verify/ /tc/verify/
RUN /tc/verify/floors.sh

FROM s-verify AS s-relocate
COPY stages/relocate.sh /tc/stages/
RUN /tc/stages/relocate.sh

# The only stage that knows which version is being cut. toolchain.env arrives
# here rather than in the base layer, where it would be an input to all 34
# stages and a version bump would rebuild every one of them.
FROM s-relocate AS s-package
COPY resolved/toolchain.env /tc/versions/
COPY stages/package.sh /tc/stages/
RUN /tc/stages/package.sh

# Nothing runs to extract this: `--output type=local` writes it straight out.
FROM scratch AS artifact
COPY --from=s-package /work/output/ /
