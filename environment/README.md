# Memgraph Build and Run Environments

## Toolchain Installation Procedure

1) Download the toolchain for your operating system from one of the following
   links (current active toolchain is `toolchain-v2`):

* [CentOS 7](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-centos-7.tar.gz)
* [CentOS 8](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-centos-8.tar.gz)
* [Debian 9](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-debian-9.tar.gz)
* [Debian 10](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-debian-10.tar.gz)
* [Ubuntu 18.04](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-ubuntu-18.04.tar.gz)
* [Ubuntu 20.04](https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v2/toolchain-v2-binaries-ubuntu-20.04.tar.gz)

2) Check and install required toolchain runtime dependencies by executing
   (e.g., on *Debian 10*):

```bash
./environment/os/debian-10.sh check TOOLCHAIN_RUN_DEPS
./environment/os/debian-10.sh install TOOLCHAIN_RUN_DEPS
```

## Toolchain Upgrade Procedure

1) Build a new toolchain for each supported OS (latest versions).
2) If the new toolchain doesn't compile on some supported OS, the last
   compilable toolchain has to be used instead. In other words, the project has
   to compile on the oldest active toolchain as well. Suppose some
   changes/improvements were added when migrating to the latest toolchain; in
   that case, the maintainer has to ensure that the project still compiles on
   previous toolchains (everything from `init` script to the actual code has to
   work on all supported operating systems).
