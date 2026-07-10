# Multi-stage Dockerfile producing two targets that share the same Memgraph
# binary:
#
#   prod            stripped binary, runtime deps only. Customer-facing image.
#   relwithdebinfo  prod + memgraph-debuginfo package + gdb / perf / libc-dbg
#                   + a source code copy + run_with_gdb.sh. Interactive-debug
#                   image.
#
# The relwithdebinfo target is layered on top of prod (FROM prod AS …) so the
# common memgraph install isn't redone — we only add the symbols and the
# debugging tooling. Build the variant you want with `docker build --target`.

###############################################################################
# python-base: shared runtime venv for both image flavours.
###############################################################################
FROM ubuntu:24.04 AS python-base
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
  python3 libpython3.12 python3-pip adduser curl binutils \
  --no-install-recommends && \
  rm -rf /var/lib/apt/lists/* /var/tmp/* && \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /etc/apt/sources.list.d/ubuntu.sources.backup ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources.backup /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  groupadd -g 103 memgraph && \
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph


COPY wheels /tmp/wheels

USER memgraph
RUN pip3 install --no-cache-dir --break-system-packages --find-links=/tmp/wheels -r /tmp/auth-module-requirements.txt && \
    pip3 install --no-cache-dir --break-system-packages numpy==1.26.4 scipy==1.13.0 networkx==3.4.2 gensim==4.3.3 xmlsec==1.3.16

###############################################################################
# prod: shipping image. Stripped memgraph binary + runtime dependencies only.
###############################################################################
FROM ubuntu:24.04 AS prod
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH
ARG CUSTOM_MIRROR

RUN --mount=type=secret,id=ubuntu_sources,target=/ubuntu.sources,required=false \
  --mount=type=bind,source="./${BINARY_NAME}${TARGETARCH}.${EXTENSION}",target=/${BINARY_NAME}${TARGETARCH}.${EXTENSION},ro \
  --mount=type=bind,source="./openssl",target=/openssl,ro \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /ubuntu.sources ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup; \
    cp -v /ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  apt-get update && apt-get install -y \
    /openssl/openssl*.deb \
    /openssl/libssl3t64*.deb \
    --no-install-recommends && \
  apt-get install -y \
    libcurl4 libseccomp2 python3 libpython3.12 libatomic1 adduser ca-certificates \
    --no-install-recommends && \
  apt install -y libxmlsec1 && \
  groupadd -g 103 memgraph && \
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph && \
  # Ubuntu Docker images exclude /usr/share/doc/* but only include copyright and changelog files
  # Add an exception for memgraph to include all files in /usr/share/doc/memgraph/
  if [ -f /etc/dpkg/dpkg.cfg.d/excludes ]; then \
    echo "" >> /etc/dpkg/dpkg.cfg.d/excludes && \
    echo "# Include all memgraph documentation files (licenses, etc.)" >> /etc/dpkg/dpkg.cfg.d/excludes && \
    echo "path-include=/usr/share/doc/memgraph/*" >> /etc/dpkg/dpkg.cfg.d/excludes; \
  fi && \
  dpkg -i "${BINARY_NAME}${TARGETARCH}.deb" && \
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

# Stable telemetry ID for containers — /etc/machine-id is regenerated on each
# container start and would over-count unique installs. Picked up at runtime
# by GetMachineId() in src/utils/system_info.cpp. This is what lets a single
# memgraph build serve both the standalone deb and the docker image.
ENV MEMGRAPH_TELEMETRY_ID=DOCKER

COPY --from=python-base --chown=memgraph:memgraph /home/memgraph/.local /home/memgraph/.local

USER memgraph
WORKDIR /usr/lib/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD []

###############################################################################
# relwithdebinfo: prod + memgraph-debuginfo + interactive debug tooling.
#
# Same memgraph binary as prod; this layer only adds:
#   * memgraph-debuginfo package — the .debug sidecars (gdb auto-finds them
#     next to the installed binaries via .gnu_debuglink).
#   * gdb + procps + perf (linux-tools-*) + libc6-dbg for live diagnosis.
#   * python3-pip / venv (some debugging scripts need them).
#   * A source-code copy so backtraces can list local frames.
#   * run_with_gdb.sh — alternative entrypoint that wraps memgraph in gdb.
###############################################################################
FROM prod AS relwithdebinfo

ARG DEBUGINFO_BINARY_NAME
ARG EXTENSION
ARG TARGETARCH
ARG SOURCE_CODE
ARG CUSTOM_MIRROR

USER root
RUN --mount=type=secret,id=ubuntu_sources,target=/ubuntu.sources,required=false \
  --mount=type=bind,source="./${DEBUGINFO_BINARY_NAME}${TARGETARCH}.${EXTENSION}",target=/${DEBUGINFO_BINARY_NAME}${TARGETARCH}.${EXTENSION},ro \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /ubuntu.sources ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.backup; \
    cp -v /ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources; \
  fi && \
  apt-get update && apt-get install -y \
    python3-pip python3.12-venv \
    gdb procps linux-tools-common linux-tools-generic libc6-dbg \
    --no-install-recommends && \
  dpkg -i "/${DEBUGINFO_BINARY_NAME}${TARGETARCH}.deb" && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
  if [ "$CUSTOM_MIRROR" = "true" ] && [ -f /etc/apt/sources.list.d/ubuntu.sources.backup ]; then \
    mv -v /etc/apt/sources.list.d/ubuntu.sources.backup /etc/apt/sources.list.d/ubuntu.sources; \
  fi

COPY "${SOURCE_CODE}" /home/mg/memgraph/src

# Alternative entrypoint that wraps memgraph in gdb. Default ENTRYPOINT
# (inherited from prod) still runs memgraph directly; users wanting gdb
# override with --entrypoint /usr/lib/memgraph/run_with_gdb.sh.
COPY run_with_gdb.sh /usr/lib/memgraph/run_with_gdb.sh

USER memgraph
