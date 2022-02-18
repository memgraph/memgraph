FROM debian:bullseye

ARG env_folder
ARG toolchain_version

COPY ${env_folder} /env_folder

RUN apt update && apt install -y curl git

RUN /${env_folder}/os/debian-11-arm.sh install MEMGRAPH_BUILD_DEPS
RUN /${env_folder}/os/debian-11-arm.sh install TOOLCHAIN_RUN_DEPS

RUN rm -rf /env_folder

RUN apt clean

RUN curl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/${toolchain_version}/${toolchain_version}-binaries-debian-11-aarch64.tar.gz -o /tmp/toolchain.tar.gz \
  && tar xvzf /tmp/toolchain.tar.gz -C /opt \
  && rm /tmp/toolchain.tar.gz
