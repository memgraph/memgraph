FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang uuid-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV BINARY_NAME memgraph_733_d9e02d6_mg_release_script_debug
ENV MEMGRAPH_CONFIG /memgraph/config/interpreter.yaml

COPY $BINARY_NAME /memgraph

WORKDIR /memgraph

CMD ./memgraph
