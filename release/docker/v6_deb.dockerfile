FROM ubuntu:24.04
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH

RUN apt-get update && apt-get install -y \
  openssl libcurl4 libssl3 libseccomp2 python3 libpython3.12 python3-pip libatomic1 adduser \
  --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# TODO(gitbuda): What are these dependencies for?
RUN pip3 install --break-system-packages  networkx==3.4.2 numpy==2.2.1 scipy==1.15.1

COPY "${BINARY_NAME}${TARGETARCH}.${EXTENSION}" /

# Install memgraph package
RUN dpkg -i "${BINARY_NAME}${TARGETARCH}.deb"

RUN pip3 install --no-cache-dir --break-system-packages -r /usr/lib/memgraph/auth_module/requirements.txt

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
