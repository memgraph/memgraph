FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV BINARY_NAME memgraph_414_dba2610_dev_debug
ENV MEMGRAPH_CONFIG /memgraph/config/memgraph.yaml

COPY $BINARY_NAME /memgraph
COPY libs/fmt /libs/fmt

WORKDIR /memgraph

CMD ./memgraph
