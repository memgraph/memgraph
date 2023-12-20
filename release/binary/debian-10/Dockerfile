ARG TOOLCHAIN_VERSION=v4
ARG OS=debian-10

FROM memgraph/memgraph-builder:${TOOLCHAIN_VERSION}_${OS}

SHELL ["/bin/bash", "-c"]

ARG OS=debian-10
ARG BUILD_TYPE=Release
ARG THREADS=8
ARG MG_TAG=master

RUN git clone https://github.com/memgraph/memgraph.git && cd memgraph && git checkout ${MG_TAG}
RUN ./memgraph/environment/os/${OS}.sh install TOOLCHAIN_RUN_DEPS && ./memgraph/environment/os/${OS}.sh install MEMGRAPH_BUILD_DEPS 
RUN source /opt/toolchain-v4/activate && cd memgraph && ./init

ENV MG_THREADS ${THREADS}
ENV MG_BUILD_TYPE ${BUILD_TYPE}

ENTRYPOINT ["sleep", "infinity"]
# ENTRYPOINT ["./memgraph/release/binary/debian-10/run.sh", "&&", "sleep", "3600"]
