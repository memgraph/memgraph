# Memgraph Release Packaging

Various tools and packaging configuration files should be put under this
directory. Common files for all packages can be kept in the root of this
directory, for example `memgraph.service`. If the common stuff should be
grouped, it should be in a subdirectory. `examples` directory is one such
case. Packaging specific stuff must have its own directory.

Currently we support distributing Memgraph binary through the following
packages.

  * Debian package
  * Docker image
  * ArchLinux package
