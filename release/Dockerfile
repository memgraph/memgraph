FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /libs
RUN mkdir -p /memgraph

ENV BINARY_NAME memgraph_393_7eff53e_demo_debug

COPY barrier_$BINARY_NAME /memgraph
COPY libs/fmt /libs/fmt

WORKDIR /memgraph

CMD ./$BINARY_NAME
