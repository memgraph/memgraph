FROM debian:10

ARG TOOLCHAIN_VERSION=4
ARG TOOLCHAIN="toolchain-v${TOOLCHAIN_VERSION}"
ARG OS=debian-10
ARG TARGETARCH=amd64

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/${TOOLCHAIN}/${TOOLCHAIN}-binaries-${OS}-${TARGETARCH}.tar.gz \
    -O ${TOOLCHAIN}-binaries-${OS}-${TARGETARCH}.tar.gz \
    && tar xzvf ${TOOLCHAIN}-binaries-${OS}-${TARGETARCH}.tar.gz -C /opt \
    && rm ${TOOLCHAIN}-binaries-${OS}-${TARGETARCH}.tar.gz

COPY . /memgraph

SHELL ["/bin/bash", "-c"]

COPY . /memgraph

RUN cd memgraph && \
    git remote set-url origin https://github.com/memgraph/memgraph.git && \
    ./environment/os/${OS}.sh install TOOLCHAIN_RUN_DEPS && \
    ./environment/os/${OS}.sh install MEMGRAPH_BUILD_DEPS && \
    source /opt/${TOOLCHAIN}/activate && \
    ./init && \
    cd / && \
    rm -rf memgraph

ENTRYPOINT ["sleep", "infinity"]
