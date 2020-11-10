# Memgraph Release Packaging

Various tools and packaging configuration files should be put under this
directory. Common files for all packages can be kept in the root of this
directory, for example `memgraph.service`. If the common stuff should be
grouped, it should be in a subdirectory. `examples` directory is one such
case. Packaging specific stuff must have its own directory.

Currently we support distributing Memgraph binary through the following
packages.

  * Debian package
  * RPM package
  * Docker image
  * ArchLinux package

## Release process

While releasing an official version of Memgraph, there are two possible
scenarios:
  * First release in new major.minor series
  * Patch release in existing major.minor series

To release a new major.minor release of Memgraph you should execute the
following steps:
  1. Merge all PRs that must be in the new release
  2. Document all changes in `CHANGELOG.md` and merge them
  3. From the `master` branch, create a branch named `release/X.Y` and push it
     to `origin`
  4. Create the release packages triggering a `Release {{Operating System}}`
     workflow using branch `release/X.Y` on Github Actions
  5. Enjoy

To release a new patch release in an existing major.minor series you should
execute the following steps:
  1. Checkout to the `release/X.Y` branch
  2. Cherry-pick all landed commits that should be included in the patch version
  3. Document all changes in `CHANGELOG.md` and commit them
  4. Edit the root `CMakeLists.txt` and set `MEMGRAPH_OVERRIDE_VERSION` to
     `X.Y.patch` and commit the change
  5. Create the release packages triggering a `Release {{Operating System}}`
     workflow using branch `release/X.Y` on Github Actions
  6. Enjoy
