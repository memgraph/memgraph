FROM fedora:36

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
RUN yum -y update \
    && yum install -y wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-fedora-36-x86_64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-fedora-36-x86_64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-fedora-36-x86_64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-fedora-36-x86_64.tar.gz

# Install toolchain run deps and memgraph build deps
SHELL ["/bin/bash", "-c"]
RUN git clone https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && ./environment/os/fedora-36.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/fedora-36.sh install MEMGRAPH_BUILD_DEPS \

ENTRYPOINT ["sleep", "infinity"]
