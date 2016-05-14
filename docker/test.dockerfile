FROM ubuntu:16.04

# apt-get setup
RUN apt-get update \
    && apt-get install -y cmake git python clang \
    && apt-get install -y check libpcre3 libpcre3-dev libjemalloc-dev \
                          libjemalloc1 build-essential libtool automake \
                          autoconf pkg-config
#    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN git clone https://pullbot:JnSdamFGKOanF1@phabricator.tomicevic.com/diffusion/MG/memgraph.git /memgraph

# update all submodules
WORKDIR /memgraph
RUN git submodule update --init

# install r3
WORKDIR /memgraph/src/speedy/r3
RUN git checkout 28726b27af3cd0a9d3166033c6619a9c7227cb48
RUN ./autogen.sh && ./configure && make

# install libuv
RUN git clone https://github.com/libuv/libuv.git /libs/libuv
WORKDIR /libs/libuv
RUN ./autogen.sh && ./configure && make && make check && make install
ENV LD_LIBRARY_PATH /usr/local/lib

# install http_parser
RUN git clone https://github.com/nodejs/http-parser /libs/http_parser
WORKDIR /libs/http_parser
# TODO: remove from here, in the time of writing the master branch
# had a bug, some not HEAD commit was checked out
RUN git checkout 4e382f96e6d3321538a78f2c7f9506d4e79b08d6
RUN make && make install

# install lexertl and compile memgraph cypher
WORKDIR /memgraph/src/cypher
RUN ./init.sh

# compile memgraph
WORKDIR /memgraph
RUN ./build.sh

# run memgraph
CMD /memgraph/memgraph 7474
