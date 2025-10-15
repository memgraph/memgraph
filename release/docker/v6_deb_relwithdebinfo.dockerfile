FROM ubuntu:24.04
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH
ARG SOURCE_CODE
ARG CUSTOM_MIRROR

# If CUSTOM_MIRROR is set, replace the default archive.ubuntu.com
# and security.ubuntu.com URIs in your .sources file
RUN if [ -n "$CUSTOM_MIRROR" ]; then \
  sed -E -i \
  -e '/^URIs:/ s#https?://[^ ]*archive\.ubuntu\.com#'"$CUSTOM_MIRROR"'#g' \
  -e '/^URIs:/ s#https?://security\.ubuntu\.com#'"$CUSTOM_MIRROR"'#g' \
  /etc/apt/sources.list.d/ubuntu.sources; \
  fi

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
  openssl libcurl4 libssl3 libseccomp2 python3 libpython3.12 python3-pip python3.12-venv libatomic1 adduser \
  gdb procps linux-tools-common linux-tools-generic linux-tools-generic libc6-dbg \
  --no-install-recommends && \
  apt install -y libxmlsec1 libdw-dev&& \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# NOTE: The following are required to run built-in Python modules. For the full
# list, please visit query_modules/CMakeLists.txt.
RUN pip3 install --break-system-packages --no-cache-dir numpy==1.26.4 scipy==1.13.0 networkx==3.4.2 gensim==4.3.3 xmlsec==1.3.16

# fix `memgraph` UID and GID for compatibility with previous Debian releases
RUN groupadd -g 103 memgraph && \
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph

COPY heaptrack /tmp/heaptrack
RUN cp -r /tmp/heaptrack/* /usr/ && rm -rf /tmp/heaptrack

COPY "${SOURCE_CODE}" /home/mg/memgraph/src

# Install memgraph package
COPY "${BINARY_NAME}${TARGETARCH}.${EXTENSION}" /
RUN dpkg -i "${BINARY_NAME}${TARGETARCH}.deb" && rm "${BINARY_NAME}${TARGETARCH}.deb"

# NOTE: The following are required to run built-in auth modules. The source of
# truth requirements file is located under
# src/auth/reference_modules/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /usr/lib/memgraph/auth_module/requirements.txt

# revert to default mirror
RUN if [ -n "$CUSTOM_MIRROR" ]; then \
  sed -E -i \
  -e "/^URIs:/ s#${CUSTOM_MIRROR}/ubuntu/#https://archive.ubuntu.com/ubuntu/#g" \
  -e "/^URIs:/ s#${CUSTOM_MIRROR}/ubuntu/#https://security.ubuntu.com/ubuntu/#g" \
  /etc/apt/sources.list.d/ubuntu.sources; \
  fi

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
