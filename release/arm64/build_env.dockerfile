FROM dokken/centos-stream-9

ARG env_folder
ARG toolchain_version

COPY ${env_folder} /env_folder

RUN yum update && yum install -y curl git

RUN /${env_folder}/os/centos-9.sh install MEMGRAPH_BUILD_DEPS
RUN /${env_folder}/os/centos-9.sh install TOOLCHAIN_RUN_DEPS

RUN rm -rf /env_folder

RUN yum clean all

RUN curl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/${toolchain_version}/${toolchain_version}-binaries-centos-9-arm64.tar.gz -o /tmp/toolchain.tar.gz \
  && tar xvzf /tmp/toolchain.tar.gz -C /opt \
  && rm /tmp/toolchain.tar.gz
