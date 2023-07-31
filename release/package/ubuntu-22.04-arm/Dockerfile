FROM ubuntu:22.04

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/${TOOLCHAIN_VERSION}/${TOOLCHAIN_VERSION}-binaries-ubuntu-22.04-arm64.tar.gz \
    -O ${TOOLCHAIN_VERSION}-binaries-ubuntu-22.04-arm64.tar.gz \
    && tar xzvf ${TOOLCHAIN_VERSION}-binaries-ubuntu-22.04-arm64.tar.gz -C /opt \
    && rm ${TOOLCHAIN_VERSION}-binaries-ubuntu-22.04-arm64.tar.gz

ENTRYPOINT ["sleep", "infinity"]
