# Memgraph Build and Run Environments

## Toolchain Upgrade Procedure

1) Build a new toolchain for each supported OS (latest versions).
2) If the new toolchain doesn't compile on some supported OS, the last
   compilable toolchain has to be used instead. In other words, the project has
   to compile on the oldest active toolchain as well. Suppose some
   changes/improvements were added when migrating to the latest toolchain; in
   that case, the maintainer has to ensure that the project still compiles on
   previous toolchains (everything from `init` script to the actual code has to
   work on all supported operating systems).
