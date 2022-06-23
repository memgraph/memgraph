FROM centos:7

ARG TOOLCHAIN_VERSION

RUN yum -y update \
    && yum install -y wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/${TOOLCHAIN_VERSION}/${TOOLCHAIN_VERSION}-binaries-centos-7-x86_64.tar.gz \
    -O ${TOOLCHAIN_VERSION}-binaries-centos-7-x86_64.tar.gz \
    && tar xzvf ${TOOLCHAIN_VERSION}-binaries-centos-7-x86_64.tar.gz -C /opt

ENTRYPOINT ["sleep", "infinity"]
