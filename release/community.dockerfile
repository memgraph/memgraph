FROM debian:stretch
# FROM debian:stretch     # 104MB
# FROM ubuntu 16.04       # 130MB
# FROM phusion/baseimage  # 220MB

ARG build_name

COPY ${build_name} /

# Setup memgraph user and group
RUN groupadd -r memgraph
RUN useradd -lrm -g memgraph memgraph
RUN chown -R memgraph:memgraph /var/log/memgraph
RUN chown -R memgraph:memgraph /var/lib/memgraph

# Memgraph listens for Bolt Protocol on this port by default.
EXPOSE 7687
# Snapshots and logging volumes
VOLUME /var/log/memgraph
VOLUME /var/lib/memgraph
# Configuration volume
VOLUME /etc/memgraph

USER memgraph
WORKDIR /home/memgraph

ENTRYPOINT ["memgraph"]
CMD [""]
