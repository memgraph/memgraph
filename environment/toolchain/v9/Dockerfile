# Build environment for memgraph toolchain v9 on Ubuntu 20.04.
#
# This image ships only the deps build.sh needs — bring the repo in yourself:
#   docker build -t mg-toolchain-v9-builder environment/toolchain/v9/
#   docker run -d --name builder mg-toolchain-v9-builder
#   docker cp . builder:/workspace/memgraph
#   docker exec -w /workspace/memgraph builder \
#       ./environment/toolchain/v9/build.sh
#   docker cp builder:/workspace/memgraph/environment/toolchain/v9/output/. ./
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
        coreutils gcc g++ build-essential make file \
        wget gnupg \
        tar gzip bzip2 xz-utils unzip \
        git \
        rsync \
        gawk bison python3 \
        m4 \
        perl \
        pkg-config \
        autoconf automake libtool \
        zlib1g-dev libbz2-dev liblzma-dev libzstd-dev \
        libdw-dev \
        libboost-filesystem-dev libboost-program-options-dev \
        libboost-iostreams-dev libboost-system-dev && \
    rm -rf /var/lib/apt/lists/*

# build.sh writes here. Pre-create so the script doesn't have to.
RUN mkdir -p /opt/toolchain-v9

WORKDIR /workspace
CMD ["/bin/bash"]
