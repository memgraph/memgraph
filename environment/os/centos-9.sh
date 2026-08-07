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
    zlib-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel pcre2-devel automake bison # for swig
    file
    openssl-devel
    gmp-devel
    gperf
    diffutils
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
    zlib # zlib library used for all builds
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
    # MAGE's query-module python deps (torch/PyG/DGL wheels) are python 3.12
    # only, so memgraph embeds python 3.12 here (built with
    # -DMG_PYTHON_VERSION=3.12) rather than the distro-default 3.9. Needs the
    # 3.12 dev headers/libs to link against, plus the interpreter + pip to
    # install the module deps at package time.
    python3.12-devel python3.12 python3.12-pip python3.12-pyyaml # for query modules
    python3-devel # for build tooling that still targets the system python
    patchelf # POST_BUILD step rewrites memgraph's DT_NEEDED for Python abi3 portability
    openssl-devel # for mgconsole (cloned + built at package time)
    python3 python3-pip python3-virtualenv nmap-ncat lsof # for qa, macro_benchmark and stress tests
    custom-rust custom-node
    rpm-build rpmlint # for RPM package building
    which nodejs golang custom-golang # for driver tests
    zip unzip java-25-openjdk java-25-openjdk-devel custom-maven # for driver tests (JDK 17 required) and neo4j (macro benchmarks)
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    ninja-build
    krb5-devel # for building python gssapi (kerberos auth module)
    xmlsec1-devel xmlsec1-openssl-devel # pip xmlsec (SAML SSO) builds from source; no wheels since 1.3.15
    sudo # stress tests set up passwordless sudo for mg (iptables)
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp
    python3.12 # embedded interpreter for query modules (see MEMGRAPH_BUILD_DEPS)
    krb5-libs # runtime for python gssapi (kerberos auth module)
)

NEW_DEPS=(
    wget curl tar gzip
)

setup_repos() {
    dnf config-manager --set-enabled crb
    # Enable EPEL for additional packages
    dnf install -y epel-release epel-next-release
    # enable rpm fusion
    dnf install --nogpgcheck -y https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-9.noarch.rpm
}

main "$@"
