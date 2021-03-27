FROM centos:7

RUN yum -y update \
    && yum install -y wget git \
    && yum clean all

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-centos-7.tar.gz \
         -O toolchain-v2-binaries-centos-7.tar.gz \
    && tar xzvf toolchain-v2-binaries-centos-7.tar.gz -C /opt

ENTRYPOINT ["sleep", "infinity"]
