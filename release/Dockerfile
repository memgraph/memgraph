FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y clang libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /libs
RUN mkdir -p /memgraph

COPY barrier /memgraph
COPY libs/fmt /libs/fmt

WORKDIR /memgraph

CMD ./memgraph_dev_f3af0ef_363_release
