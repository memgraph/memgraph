FROM rockylinux/rockylinux:10

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
RUN yum -y update \
    && yum install -y wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q --max-redirect=0 https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz

# Install toolchain run deps and memgraph build deps
ARG GIT_REF
SHELL ["/bin/bash", "-c"]
RUN git clone --branch ${GIT_REF} https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && if ! grep -q '^MG_NVM_COMMIT=' environment/util.sh; then \
         echo "environment/util.sh at ${GIT_REF} predates the pinned rust/node installers" >&2; \
         exit 1; \
       fi \
    && ./environment/os/rocky-10.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/rocky-10.sh install MEMGRAPH_BUILD_DEPS \
    && ./environment/os/rocky-10.sh install MEMGRAPH_TEST_DEPS \
    && ./environment/os/rocky-10.sh install MEMGRAPH_RUN_DEPS \
    && cp environment/util.sh /tmp/util.sh \
    && cd .. && rm -rf memgraph

# Create mg user and set as default
RUN useradd -m -s /bin/bash mg \
    && chown mg: /tmp/util.sh
USER mg

# Install rust and node for mg.
ARG RUST_VERSION
ARG NODE_VERSION
# util.sh falls back to its own MG_*_VERSION defaults when these are empty.
RUN export MG_RUST_VERSION="${RUST_VERSION}" MG_NODE_VERSION="${NODE_VERSION}" \
    && source /tmp/util.sh \
    && retry_install install_rust "$MG_RUST_VERSION" \
    && retry_install install_node "$MG_NODE_VERSION" \
    && rm /tmp/util.sh

ENTRYPOINT ["sleep", "infinity"]
