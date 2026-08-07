#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils-common gcc gcc-c++ make binutils-gold # generic build tools
    wget2-wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    # NOTE: https://discussion.fedoraproject.org/t/f40-change-proposal-transitioning-to-zlib-ng-as-a-compatible-replacement-for-zlib-system-wide/95807
    zlib-ng-compat-devel zlib-ng-compat-static # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    curl libcurl-devel # for cmake
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel pcre2-devel automake bison # for swig
    file
    openssl openssl-devel openssl-devel-engine # for pulsar
    gmp-devel
    gperf
    diffutils
    patch
    perl # for openssl
    git
    custom-rust # for mgcxx
    libtool # for protobuf
    pkgconf-pkg-config # for pulsar
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
    wget2-wget tar gzip # for downloading and unpacking libs (curl binary ships via curl-minimal)
    perl # conan openssl's Configure
    gperf # conan libseccomp source build
    readline-devel # optional readline support (manual tests)
    python3-devel # for query modules
    patchelf # POST_BUILD step rewrites memgraph's DT_NEEDED for Python abi3 portability
    openssl-devel # for mgconsole (cloned + built at package time)
    python3 python3-pip python3-virtualenv # for conan (runs in a venv) and build tooling
    python3-pyyaml
    rpm-build rpmlint # for RPM package building
    which # needed by various build and test scripts
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    ninja-build
    krb5-devel # for building python gssapi (kerberos auth module)
)

# Extra packages on top of MEMGRAPH_BUILD_DEPS needed to run the test suites.
MEMGRAPH_TEST_DEPS=(
    java-25-openjdk java-25-openjdk-devel # for driver tests and neo4j (macro benchmarks)
    nmap-ncat lsof # for qa, macro_benchmark and stress tests
    nodejs golang custom-golang # for driver tests
    zip unzip custom-maven # for driver tests
    xmlsec1-devel xmlsec1-openssl-devel # pip xmlsec (SAML SSO) builds from source; no wheels since 1.3.15
    sudo # stress tests set up passwordless sudo for mg (iptables)
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp
    krb5-libs # runtime for python gssapi (kerberos auth module)
)


setup_repos() {
    # enable rpm fusion
    dnf install -y https://mirrors.rpmfusion.org/free/fedora/rpmfusion-free-release-42.noarch.rpm
}

main "$@"
