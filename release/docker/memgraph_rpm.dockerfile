FROM dokken/centos-stream-9
# NOTE: If you change the base distro update release/package as well.

ARG release

RUN yum update && yum install -y \
    openssl libcurl libseccomp python3 python3-pip \
    --nobest --allowerasing \
  && rm -rf /tmp/* \
  && yum clean all

RUN pip3 install networkx==2.4 numpy==1.21.4 scipy==1.7.3

COPY ${release} /

# Install memgraph package
RUN rpm -i ${release}

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
