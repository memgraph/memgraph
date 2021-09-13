FROM debian:buster
# NOTE: If you change the base distro update release/package as well.

ARG deb_release

RUN apt-get update && apt-get install -y \
  openssl libcurl4 libssl1.1 libseccomp2 python3 libpython3.7 python3-pip \
  --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install networkx==2.4 numpy==1.19.2 scipy==1.5.2

COPY ${deb_release} /

# Install memgraph package
RUN dpkg -i ${deb_release}

# Memgraph listens for Bolt Protocol on this port by default.
EXPOSE 7687

USER memgraph
WORKDIR /usr/lib/memgraph

ENTRYPOINT ["/usr/lib/memgraph/memgraph"]
CMD [""]
