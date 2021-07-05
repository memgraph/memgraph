FROM centos:7

RUN yum -y update \
    && yum install -y wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v3/toolchain-v3-binaries-centos-7.tar.gz \
         -O toolchain-v3-binaries-centos-7.tar.gz \
    && tar xzvf toolchain-v3-binaries-centos-7.tar.gz -C /opt

ENTRYPOINT ["sleep", "infinity"]
