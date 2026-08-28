# Multi-stage Dockerfile producing three targets:
#
#   prod            stripped binary, runtime deps only. Customer-facing image.
#   relwithdebinfo  prod + memgraph-debuginfo package + gdb / perf / libc-dbg
#                   + a source code copy + run_with_gdb.sh. Interactive-debug
#                   image.
#   prod-fips       FIPS 140-3 variant: the validated OpenSSL FIPS provider in
#                   approved mode, and a Memgraph built without Python.
#
# The relwithdebinfo target is layered on top of prod (FROM prod AS …) so the
# common memgraph install isn't redone — we only add the symbols and the
# debugging tooling. Build the variant you want with `docker build --target`.
#
# prod-fips deliberately does NOT layer on prod; see the comment on that stage.

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
RUN pip3 install --no-cache-dir --break-system-packages --find-links=/tmp/wheels --only-binary=gssapi -r /tmp/auth-module-requirements.txt && \
    pip3 install --no-cache-dir --break-system-packages numpy==1.26.4 scipy==1.13.0 networkx==3.4.2 xmlsec==1.3.16

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
    libcurl4 libseccomp2 python3 python3-pip libpython3.12 libatomic1 adduser ca-certificates \
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
  # MG_SKIP_PYTHON_DEPS: the postinst would pip-install the query-module
  # python deps into the image layer, duplicating what the python-base
  # stage already provides (COPYed into /home/memgraph/.local below).
  MG_SKIP_PYTHON_DEPS=1 dpkg -i "${BINARY_NAME}${TARGETARCH}.deb" && \
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

###############################################################################
# prod-fips: FIPS 140-3 variant.
#
# Differs from prod in three ways, all of which force a parallel stage rather
# than `FROM prod`:
#
#   1. OpenSSL. The openssl-fips-provider package Depends on an exact
#      libssl3t64 version (…+fipsN.N.N) that differs from the stock one prod
#      installs, so it cannot be layered on top.
#   2. Python. The Python auth-module wheels (cryptography, xmlsec) embed their
#      own statically linked OpenSSL — a second, unvalidated crypto module
#      inside the image, on the SAML/JWT auth path. prod COPYs them in as a
#      layer, and a layer cannot be removed by a descendant stage.
#   3. The Memgraph package itself is a -DMG_PYTHON_SUPPORT=OFF build, so its
#      dependency set and postinst differ from prod's.
###############################################################################
FROM ubuntu:24.04 AS prod-fips

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
  apt-get update && \
  apt-get install -y \
    /openssl/openssl*.deb \
    /openssl/libssl3t64*.deb \
    --no-install-recommends && \
  apt-get install -y \
    libcurl4 libseccomp2 libatomic1 adduser ca-certificates \
    --no-install-recommends && \
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

# Approved mode is opt-in per process via OPENSSL_CONF; the provider package
# installs the module but activates nothing. openssl-fips.cnf activates fips +
# base only (no default provider), sets default_properties=fips=yes, and pins
# MinProtocol to TLSv1.2. fipsmodule.cnf next to it carries the HMAC-SHA256
# integrity value over fips.so and the approved-mode settings.
ENV OPENSSL_CONF=/etc/ssl/openssl-fips.cnf

# Fail the build rather than ship an image that silently is not in approved
# mode. Everything here is cheap and catches the failure modes that would
# otherwise surface as a container that won't start (exit 14) or, worse, one
# that starts and quietly uses unvalidated crypto.
RUN set -eu; \
  modulesdir="$(openssl version -m | sed -e 's/^MODULESDIR: //' -e 's/"//g')"; \
  echo "MODULESDIR=${modulesdir}"; \
  # Proves libcrypto searches where the provider package installed fips.so
  # (/usr/lib/ossl-modules is a symlink to the multiarch dir). If these ever
  # diverge, set OPENSSL_MODULES rather than moving the module.
  test -f "${modulesdir}/fips.so" \
    || { echo "fips.so is not in libcrypto's MODULESDIR (${modulesdir})" >&2; exit 1; }; \
  openssl list -providers | grep -q "OpenSSL FIPS Provider" \
    || { echo "FIPS provider is not active under ${OPENSSL_CONF}" >&2; exit 1; }; \
  # Loaded is not the same as operational: a module that fails a power-on
  # self-test still appears in the list, with a non-active status.
  openssl list -providers | grep -A3 "^  fips" | grep -q "status: active" \
    || { echo "FIPS provider is present but not operational" >&2; exit 1; }; \
  # The DRBG is instantiated lazily and cached for the process lifetime, so a
  # default-provider DRBG here would mean every salt and nonce came from an
  # unvalidated source while everything else looked correct.
  openssl list -random-instances | grep -q "@ fips" \
    || { echo "DRBG is not being supplied by the FIPS provider" >&2; exit 1; }; \
  # Approved mode must actually remove non-approved algorithms, not merely
  # prefer approved ones.
  echo test | openssl dgst -sha256 >/dev/null \
    || { echo "SHA-256 unavailable in approved mode" >&2; exit 1; }; \
  if echo test | openssl dgst -md5 >/dev/null 2>&1; then \
    echo "MD5 is still reachable — the default provider is active" >&2; exit 1; \
  fi; \
  echo "FIPS approved mode verified."

# Memgraph listens for Bolt Protocol on this port by default.
EXPOSE 7687
# Snapshots and logging volumes
VOLUME /var/log/memgraph
VOLUME /var/lib/memgraph
# Configuration volume
VOLUME /etc/memgraph

ENV MEMGRAPH_TELEMETRY_ID=DOCKER

USER memgraph
WORKDIR /usr/lib/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD []
