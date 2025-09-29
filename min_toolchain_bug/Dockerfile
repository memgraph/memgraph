
# Ubuntu 24.04 (Noble)
FROM ubuntu:24.04

# Avoid interactive tzdata/etc during apt
ENV DEBIAN_FRONTEND=noninteractive

# Pin versions as args for easy updates
ARG UBUNTU_CODENAME=noble
ARG LLVM_VERSION=20

# System deps + add the official LLVM APT repo & key, then install Clang 20 + libc++20
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg \
      software-properties-common wget \
      build-essential cmake pkg-config \
    ; \
    wget https://apt.llvm.org/llvm.sh; \
    chmod +x llvm.sh; \
    ./llvm.sh ${LLVM_VERSION} all; \
    rm -rf /var/lib/apt/lists/*

# Make clang-20 the default compiler in the container shell
ENV CC=clang-20 \
    CXX=clang++-20

# Optional: verify toolchain at build time (kept as a layer for debugging)
RUN set -eux; \
    clang-20 --version; \
    clang++-20 --version

RUN apt update && apt install -y python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*

# Default workdir
WORKDIR /work

RUN python3 -m venv env && \
    . env/bin/activate && \
    pip install conan

COPY profile .
RUN /bin/bash -c "source env/bin/activate && conan profile detect && conan install --requires boost/1.86.0 --build=missing -pr ./profile"

# Default command (interactive shell)
CMD ["/bin/bash"]
