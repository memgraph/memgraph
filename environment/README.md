# Memgraph Operating Environments

## Issues related to build toolchain

* GCC 11.2 (toolchain-v4) doesn't compile on Fedora 38, multiple definitions of enum issue
* spdlog 1.10/11 doesn't work with fmt 10.0.0

## os

Under the `os` directory, you can find scripts to install all required system
dependencies on operating systems where Memgraph natively builds. The testing
script helps to see how to install all packages (in the case of a new package),
or make any adjustments in the overall system setup. Also, the testing script
helps check if Memgraph runs on a freshly installed operating system (with no
packages installed).
