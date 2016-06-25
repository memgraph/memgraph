FROM ubuntu:16.04

# apt-get setup
RUN apt-get update \
    && apt-get install -y cmake git python clang wget \
    && apt-get install -y check libpcre3 libpcre3-dev libjemalloc-dev \
                          libjemalloc1 build-essential libtool automake \
                          autoconf pkg-config
#    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# prepare source
RUN mkdir -p /memgraph/build
RUN mkdir -p /memgraph/libs
COPY libs/setup.sh /memgraph/libs/setup.sh
COPY src /memgraph/src
COPY tests /memgraph/tests
COPY CMakeLists.txt /memgraph/CMakeLists.txt

# setup libs
WORKDIR /memgraph/libs
RUN ./setup.sh

# build
WORKDIR /memgraph/build
RUN cmake -DCMAKE_C_COMPILER=clang \
          -DCMAKE_CXX_COMPILER=clang++ \
          -DRUNTIME_ASSERT=OFF \
          -DTHROW_EXCEPTION_ON_ERROR=OFF \
          -DNDEBUG=OFF ..
RUN make

# run
CMD /memgraph/build/memgraph 7474
