ARG TOOLCHAIN_VERSION
ARG OS

FROM memgraph/memgraph-builder:${TOOLCHAIN_VERSION}_${OS}

SHELL ["/bin/bash", "-c"]

ARG OS

COPY . /memgraph

RUN cd memgraph && \
    git remote set-url origin https://github.com/memgraph/memgraph.git && \
    ./environment/os/${OS}.sh install TOOLCHAIN_RUN_DEPS && \
    ./environment/os/${OS}.sh install MEMGRAPH_BUILD_DEPS && \
    source /opt/toolchain-v4/activate && \
    ./init

# Sleep for 2 hours
ENTRYPOINT ["sleep", "7200"]
