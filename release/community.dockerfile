FROM debian:stretch

ARG deb_release

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
WORKDIR /home/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD [""]
