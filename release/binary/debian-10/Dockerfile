ARG TOOLCHAIN_VERSION=v4
ARG OS=debian-10

FROM memgraph/memgraph-builder:${TOOLCHAIN_VERSION}_${OS}

ARG OS=debian-10
ARG BUILD_TYPE=Release
ARG THREADS=8

RUN git clone https://github.com/memgraph/memgraph.git && git checkout jepsen-upgrade

RUN ./memgraph/environment/os/${OS}.sh install TOOLCHAIN_RUN_DEPS && ./memgraph/environment/os/${OS}.sh install MEMGRAPH_BUILD_DEPS 

ENV MG_THREADS ${THREADS}
ENV MG_BUILD_TYPE ${BUILD_TYPE}

ENTRYPOINT ["./memgraph/release/binary/debian-10/run.sh", "&&", "sleep", "3600"]
