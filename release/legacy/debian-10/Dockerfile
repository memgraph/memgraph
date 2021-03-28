FROM debian:10

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-debian-10.tar.gz \
         -O toolchain-v2-binaries-debian-10.tar.gz \
    && tar xzvf toolchain-v2-binaries-debian-10.tar.gz -C /opt

ENTRYPOINT ["sleep", "infinity"]
