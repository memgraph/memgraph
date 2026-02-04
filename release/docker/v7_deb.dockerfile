FROM ubuntu:24.04 AS python-base
# This stage will create the python venv used for the runtime in `prod`
ARG CUSTOM_MIRROR=false
ARG TARGETARCH
ARG CACHE_PRESENT=false
ENV DEBIAN_FRONTEND=noninteractive

USER root
COPY auth-module-requirements.txt /tmp/auth-module-requirements.txt
RUN --mount=type=secret,id=ubuntu_sources,target=/ubuntu.sources,required=false \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /ubuntu.sources ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup; \
    cp -v /ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  apt-get update && apt-get install -y \
  python3 libpython3.12 python3-pip adduser curl \
  --no-install-recommends && \
  rm -rf /var/lib/apt/lists/* /var/tmp/* && \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /etc/apt/sources.list.d/ubuntu.sources.backup ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources.backup /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  groupadd -g 103 memgraph && \
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph


USER memgraph
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/auth-module-requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages numpy==1.26.4 scipy==1.13.0 networkx==3.4.2 gensim==4.3.3 xmlsec==1.3.16

FROM ubuntu:24.04
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH
ARG CUSTOM_MIRROR

COPY "${BINARY_NAME}${TARGETARCH}.${EXTENSION}" /
COPY openssl/* /tmp/openssl/
RUN --mount=type=secret,id=ubuntu_sources,target=/ubuntu.sources,required=false \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /ubuntu.sources ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup; \
    cp -v /ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  apt-get update && apt-get install -y \
    /tmp/openssl/openssl*.deb \
    /tmp/openssl/libssl3t64*.deb \
    --no-install-recommends && \
  apt-get install -y \
    libcurl4 libseccomp2 python3 libpython3.12 libatomic1 adduser \
    --no-install-recommends && \
    apt install -y libxmlsec1 && \
  groupadd -g 103 memgraph && \
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph && \
  dpkg -i "${BINARY_NAME}${TARGETARCH}.deb" && rm "${BINARY_NAME}${TARGETARCH}.deb" && \
  apt remove adduser -y && \
  apt autoremove -y && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /etc/apt/sources.list.d/ubuntu.sources.backup ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources.backup /etc/apt/sources.list.d/ubuntu.sources; \
  fi

# Memgraph listens for Bolt Protocol on this port by default.
EXPOSE 7687
# Snapshots and logging volumes
VOLUME /var/log/memgraph
VOLUME /var/lib/memgraph
# Configuration volume
VOLUME /etc/memgraph

COPY --from=python-base --chown=memgraph:memgraph /home/memgraph/.local/lib/python3.12/site-packages /home/memgraph/.local/lib/python3.12/site-packages

USER memgraph
WORKDIR /usr/lib/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD [""]
