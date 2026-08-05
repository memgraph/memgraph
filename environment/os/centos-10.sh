#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

TOOLCHAIN_BUILD_DEPS=(
    wget # used for archive download
    coreutils-common gcc gcc-c++ make # generic build tools
    # NOTE: Pure libcurl conflicts with libcurl-minimal
    libcurl-devel # cmake build requires it
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-ng-compat-devel zlib-ng-compat-static # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre2-devel automake bison # for swig
    file gmp-devel gperf diffutils
#    libipt libipt-devel # intel TODO(matt): add to toolchain sysroot
    patch
    custom-rust # for mgcxx
    libtool # for protobuf
    openssl-devel pkgconf-pkg-config # for pulsar
    cyrus-sasl-devel # for librdkafka
    python3-pip # for conan
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib-ng-compat # zlib library used for all builds
    python3 # llvm helper scripts; gdb ships its own python via the sysroot
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    gcc-c++ libstdc++-devel libstdc++-static # conan tool builds (ninja links -static-libstdc++)
    make cmake pkgconf-pkg-config # build system
    wget # for downloading libs
    perl # conan openssl's Configure
    gperf # conan libseccomp source build
    readline-devel # optional readline support (manual tests)
    python3-devel # for query modules
    patchelf # POST_BUILD step rewrites memgraph's DT_NEEDED for Python abi3 portability
    openssl-devel # for mgconsole (cloned + built at package time)
    python3 python3-pip python3-virtualenv nmap-ncat lsof # for qa, macro_benchmark and stress tests
    python3-pyyaml
    custom-rust
    rpm-build rpmlint # for RPM package building
    which nodejs golang custom-golang # for driver tests
    zip unzip java-25-openjdk-headless java-25-openjdk java-25-openjdk-devel custom-maven # for driver tests and neo4j (macro benchmarks)
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    ninja-build
    krb5-devel # for building python gssapi (kerberos auth module)
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp
    krb5-libs # runtime for python gssapi (kerberos auth module)
)

NEW_DEPS=(
    wget curl tar gzip
)

setup_repos() {
    # CRB repo is required for, e.g. texinfo, ninja-build
    dnf config-manager --set-enabled crb
    # EPEL is required for, e.g. rpmlint, python3-virtualenv
    dnf install -y epel-release
    # enable rpm fusion
    dnf install --nogpgcheck -y https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-10.noarch.rpm
}

main "$@"
