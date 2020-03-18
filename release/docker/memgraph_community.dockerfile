FROM debian:stretch

ARG deb_release

RUN apt-get update && apt-get install -y \
    openssl libcurl3 libssl1.1 python3 libpython3.5 \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ${deb_release} /

# Install memgraph package
RUN dpkg -i ${deb_release}

# Memgraph listens for Bolt Protocol on this port by default.
EXPOSE 7687
# Snapshots and logging volumes
VOLUME /var/log/memgraph
VOLUME /var/lib/memgraph
# Configuration volume
VOLUME /etc/memgraph

USER memgraph
WORKDIR /usr/lib/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD [""]
