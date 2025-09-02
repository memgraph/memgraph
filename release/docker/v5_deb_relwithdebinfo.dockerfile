FROM debian:12
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH
ARG SOURCE_CODE

RUN apt-get update && apt-get install -y \
  openssl libcurl4 libssl3 libseccomp2 python3 libpython3.11 python3-pip gdb procps linux-perf libc6-dbg libxmlsec1-dev xmlsec1 \
  --no-install-recommends && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --break-system-packages  numpy==1.26.4 scipy==1.12.0 networkx==3.2.1 gensim==4.3.3

COPY "${BINARY_NAME}${TARGETARCH}.${EXTENSION}" /
COPY "${SOURCE_CODE}" /home/mg/memgraph/src

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
