ARG TOOLCHAIN_VERSION=4
ARG OS=debian-10

FROM mgbuilder-base:v${TOOLCHAIN_VERSION}_${OS}

SHELL ["/bin/bash", "-c"]

ARG TOOLCHAIN_VERSION=4
ARG OS=debian-10
ARG BUILD_TYPE=Release
ARG THREAD_COUNT=8

COPY . /memgraph

RUN cd memgraph && \
    git remote set-url origin https://github.com/memgraph/memgraph.git && \
    source /opt/toolchain-v${TOOLCHAIN_VERSION}/activate && \
    ./init && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=$MG_BUILD_TYPE -DMG_ENTERPRISE=OFF ..

RUN cd  memgraph/build && \
    source /opt/toolchain-v${TOOLCHAIN_VERSION}/activate && \
    make -j$THREAD_COUNT

# Sleep for 2 hours
ENTRYPOINT ["sleep", "7200"]
