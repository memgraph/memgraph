ARG TOOLCHAIN_VERSION=4
ARG OS=debian-10

FROM memgraph/mgbuilder-base:toolchain-v${TOOLCHAIN_VERSION}_${OS}

ARG BUILD_TYPE=Release
ARG THREADS
ENV BUILD_TYPE=${BUILD_TYPE}
ENV THREADS=${THREADS}
ARG CMAKE_CMD="cmake -DCMAKE_BUILD_TYPE=$BUILD_TYPE .."
ARG MAKE_CMD="make -j$THREADS"

COPY . /memgraph

SHELL ["/bin/bash", "-c"]

# Initialize deps
RUN cd memgraph && \
    git remote set-url origin https://github.com/memgraph/memgraph.git && \
    $TOOLCHAIN_ACTIVATE && \
    ./init && \
    cd build && \
    $CMAKE_CMD && \
    $MAKE_CMD

ENTRYPOINT ["sleep", "infinity"]
