ARG TOOLCHAIN_VERSION=v4
ARG THREADS=8
ARG BUILD_TYPE=Release
ARG OS=debian-10

FROM memgraph/memgraph-builder:${TOOLCHAIN_VERSION}_${OS}

COPY . memgraph/

RUN ./memgraph/environment/os/${OS}.sh install TOOLCHAIN_RUN_DEPS && ./memgraph/environment/os/${OS}.sh install MEMGRAPH_BUILD_DEPS 

ENV MG_THREADS  ${THREADS}
ENV MG_BUILD_TYPE  ${BUILD_TYPE}

ENTRYPOINT ["./run.sh"]
