ARG TOOLCHAIN_VERSION
ARG BRANCH=master
ARG THREADS=24
ARG BUILD_TYPE=RelWithDebInfo
FROM memgraph/memgraph-builder:v${TOOLCHAIN_VERSION}_debian-10

# Clone memgraph
RUN git clone -b ${BRANCH} git@github.com:memgraph/memgraph.git

# Install dependencies and build binaries
RUN source /opt/toolchain-v4/activate && \
  cd memgraph && \
  ./init && \
  cd build && \
  cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DMG_ENTERPRISE=OFF .. && \
  make -j${THREADS}

ENTRYPOINT ["sleep", "eternity"]
