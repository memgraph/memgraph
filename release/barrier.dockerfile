FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /libs

ENV BINARY_NAME memgraph_414_dba2610_dev_debug

COPY barrier_$BINARY_NAME /memgraph
COPY libs/fmt /libs/fmt

WORKDIR /memgraph

CMD ./$BINARY_NAME
