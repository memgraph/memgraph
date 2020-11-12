# Quick Start

A short chapter on downloading the Memgraph source, compiling and running.

## Obtaining the Source Code

Memgraph uses `git` for source version control. You will need to install `git`
on your machine before you can download the source code.

On Debian systems, you can do it inside a terminal with the following
command:

    sudo apt-get install git

On ArchLinux or Gentoo, you probably already know what to do.

After installing `git`, you are now ready to fetch your own copy of Memgraph
source code. Run the following command:

    git clone https://github.com/memgraph/memgraph.git

The above will create a `memgraph` directory and put all source code there.

## Compiling Memgraph

With the source code, you are now ready to compile Memgraph. Well... Not
quite. You'll need to download Memgraph's dependencies first.

In your terminal, position yourself in the obtained memgraph directory.

    cd memgraph

### Installing Dependencies

Dependencies that are required by the codebase should be checked by running the
`init` script:

    ./init

If the script fails, dependencies installation scripts could be found under
`environment/os/`. The directory contains dependencies management script for
each supported operating system. E.g. if your system is Debian 10, run the
following to install all required build packages:

    ./environment/os/debian-10.sh install MEMGRAPH_BUILD_DEPS

Once everything is installed, rerun the `init` script.

Once the `init` script is successfully finished, issue the following commands:

    mkdir -p build
    ./libs/setup.sh

### Compiling

Memgraph is compiled using our own custom toolchain that can be obtained from
the toolchain repository. You should read the `environment/README.txt` file
in the repository and install the apropriate toolchain for your distribution.
After you have installed the toolchain you should read the instructions for the
toolchain in the toolchain install directory (`/opt/toolchain-vXYZ/README.md`)
and install dependencies that are necessary to run the toolchain.

When you want to compile Memgraph you should activate the toolchain using the
prepared toolchain activation script that is also described in the toolchain
`README`.

NOTE: You *must* activate the toolchain every time you want to compile
Memgraph!

You should now activate the toolchain in your console.

    source /opt/toolchain-vXYZ/activate

With all of the dependencies installed and the build environment set-up, you
need to configure the build system. To do that, execute the following:

    cd build
    cmake ..

If everything went OK, you can now, finally, compile Memgraph.

    make -j$(nproc)

### Running

After the compilation verify that Memgraph works:

    ./memgraph --version

To make extra sure, run the unit tests:

    ctest -R unit -j$(nproc)

## Problems

If you have any trouble running the above commands, contact your nearest
developer who successfully built Memgraph. Ask for help and insist on getting
this document updated with correct steps!

## Next Steps

Familiarise yourself with our code conventions and guidelines:

  * [C++ Code](cpp-code-conventions.md)
  * [Other Code](other-code-conventions.md)
  * [Code Review Guidelines](code-review.md)

Take a look at the list of [required reading](required-reading.md) for
brushing up on technical skills.
