FROM debian:10

RUN apt update && apt install -y \
    ca-certificates wget git \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-debian-10.tar.gz \
         -O toolchain-v2-binaries-debian-10.tar.gz \
    && tar xzvf toolchain-v2-binaries-debian-10.tar.gz -C /opt

ENTRYPOINT ["sleep", "infinity"]
