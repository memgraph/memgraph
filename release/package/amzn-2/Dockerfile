FROM amazonlinux:2

ARG TOOLCHAIN_VERSION

RUN yum -y update \
    && yum install -y wget git tar
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

# Download and install toolchain
RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-amzn-2-x86_64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-amzn-2-x86_64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-amzn-2-x86_64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-amzn-2-x86_64.tar.gz

# Install toolchain run deps and memgraph build deps
SHELL ["/bin/bash", "-c"]
RUN git clone https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && ./environment/os/amzn-2.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/amzn-2.sh install MEMGRAPH_BUILD_DEPS \
    && cd .. && rm -rf memgraph

# Create mg user and set as default
RUN useradd -m -s /bin/bash mg
USER mg

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENTRYPOINT ["sleep", "infinity"]
