FROM ubuntu:24.04
# NOTE: If you change the base distro update release/package as well.

ARG BINARY_NAME
ARG EXTENSION
ARG TARGETARCH

RUN apt-get update && apt-get install -y \
  openssl libcurl4 libssl3 libseccomp2 python3 libpython3.12 python3-pip libatomic1 adduser \
  --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Bugfix for timezone issues - pin the package
RUN apt install tzdata=2024a-2ubuntu1
RUN sudo apt-mark hold tzdata

# NOTE: The following are required to run built-in Python modules. For the full
# list, please visit query_modules/CMakeLists.txt.
RUN pip3 install --break-system-packages  numpy==1.26.4 scipy==1.13.0 networkx==3.4.2 gensim==4.3.3

COPY "${BINARY_NAME}${TARGETARCH}.${EXTENSION}" /

# Install memgraph package
RUN dpkg -i "${BINARY_NAME}${TARGETARCH}.deb"

# NOTE: The following are required to run built-in auth modules. The source of
# truth requirements file is located under
# src/auth/reference_modules/requirements.txt
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
