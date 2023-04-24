FROM debian:10

ARG TOOLCHAIN_VERSION=toolchain-v4

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/${TOOLCHAIN_VERSION}/${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz \
    -O ${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz \
    && tar xzvf ${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz -C /opt \
    && rm ${TOOLCHAIN_VERSION}/${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz

RUN git clone https://github.com/memgraph/memgraph.git \
    && ./memgraph/environments/os/debian-10.sh install TOOLCHAIN_RUN_DEPS \
    && ./memgraph/environment/os/debian-10.sh install MEMGRAPH_BUILD_DEPS

RUN cd memgraph \
    && source /opt/toolchain-v4/activate \
    && ./init \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make -j4 \
    && make -j4 mgconsole

ENTRYPOINT ["sleep", "infinity"]
