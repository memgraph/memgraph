FROM debian:10

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-debian-10-amd64.tar.gz

# Install toolchain run deps and memgraph build deps
SHELL ["/bin/bash", "-c"]
RUN git clone https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && ./environment/os/debian-10.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/debian-10.sh install MEMGRAPH_BUILD_DEPS \
    && cd .. && rm -rf memgraph

# Create mg user and set as default
RUN useradd -m -s /bin/bash mg
USER mg

ENTRYPOINT ["sleep", "infinity"]
