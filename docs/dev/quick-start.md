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

    git clone https://phabricator.memgraph.io/diffusion/MG/memgraph.git

The above will create a `memgraph` directory and put all source code there.

## Compiling Memgraph

With the source code, you are now ready to compile Memgraph. Well... Not
quite. You'll need to download Memgraph's dependencies first.

In your terminal, position yourself in the obtained memgraph directory.

    cd memgraph

### Installing Dependencies

On Debian systems, all of the dependencies should be setup by running the
`init` script:

    ./init -s

Currently, other systems aren't supported in the `init` script. But you can
issue the needed steps manually. First run the `init` script.

    ./init

The script will output the required packages, which you should be able to
install via your favorite package manager. For example, `pacman` on ArchLinux.
After installing the packages, issue the following commands:

    mkdir -p build
    ./libs/setups.sh

### Compiling

With all of the dependencies installed, you need to configure the build
system. To do that, execute the following:

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
