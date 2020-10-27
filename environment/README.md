# Memgraph Build and Run Environments

## Toolchain Upgrade Procedure

1) Build a new toolchain for each supported OS (latest versions).
2) If the new toolchain doesn't compile on some non-latest OS, last compilable
   toolchain has to be used instead. In other words, project has to compile on
   the oldest active toolchain as well. If some changes/improvements were addad
   when migrating to the latest toolchain, the maintainer has to ensure that
   the project still compiles on previous toolchains (probably init or
   lib/setup.sh, maybe some of the CMakeLists.txt files have to updated).
