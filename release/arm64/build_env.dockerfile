FROM dokken/centos-stream-9

RUN yum update && yum install -y curl git

RUN curl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v4/toolchain-v4-binaries-centos-9-arm64.tar.gz -o /tmp/toolchain.tar.gz \
  && tar xvzf /tmp/toolchain.tar.gz -C /opt \
  && rm /tmp/toolchain.tar.gz
