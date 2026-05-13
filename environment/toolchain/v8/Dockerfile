# Build environment for memgraph toolchain v8 on Ubuntu 20.04.
#
# This image ships only the deps build.sh needs — bring the repo in yourself:
#   docker build -t mg-toolchain-v8-builder environment/toolchain/v8/
#   docker run -d --name builder mg-toolchain-v8-builder
#   docker cp . builder:/workspace/memgraph
#   docker exec -w /workspace/memgraph builder \
#       ./environment/toolchain/v8/build.sh
#   docker cp builder:/workspace/memgraph/environment/toolchain/v8/output/. ./
#
# The package list mirrors TOOLCHAIN_BUILD_DEPS in
# environment/os/ubuntu-20.04.sh — keep both sides in sync.

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LANG=C.UTF-8

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        coreutils gcc g++ build-essential make \
        wget curl \
        gnupg \
        tar gzip bzip2 xz-utils unzip \
        zlib1g-dev \
        libexpat1-dev libipt-dev libbabeltrace-dev liblzma-dev python3-dev texinfo \
        libcurl4-openssl-dev \
        libreadline-dev \
        libffi-dev libxml2-dev \
        file \
        git \
        libgmp-dev \
        gperf \
        libssl-dev \
        libedit-dev libpcre2-dev libpcre3-dev automake bison \
        libtool pkg-config \
        libsasl2-dev \
        python3 python3-pip \
        gawk \
        rsync && \
    rm -rf /var/lib/apt/lists/*

# Rust toolchain — build.sh sources $HOME/.cargo/env unconditionally even
# though the toolchain itself doesn't compile Rust. Version mirrors
# install_rust in environment/util.sh.
ARG RUST_VERSION=1.85
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --profile minimal --default-toolchain ${RUST_VERSION} && \
    /root/.cargo/bin/rustc --version
ENV PATH=/root/.cargo/bin:${PATH}

# build.sh writes here. Pre-create so the script doesn't have to.
RUN mkdir -p /opt/toolchain-v8

WORKDIR /workspace
CMD ["/bin/bash"]
