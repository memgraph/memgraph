FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang uuid-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV MEMGRAPH_CONFIG /memgraph/config/interpreter.yaml
ARG build_name
COPY ${build_name} /memgraph

WORKDIR /memgraph
CMD ./memgraph
