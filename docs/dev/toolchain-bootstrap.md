# Bootstrapping Compilation Toolchain for Memgraph

Requirements:

  * libstdc++ shipped with gcc-6.3 or gcc-6.4
  * cmake >= 3.1, Debian Stretch uses cmake-3.7.2
  * clang-3.9

## Installing gcc-6.4

gcc-6.3 has a bug, so use the 6.4 version which is just a bugfix release.

Requirements on CentOS 7:

  * wget
  * make
  * gcc (bootstrap)
  * gcc-c++ (bootstrap)
  * gmp-devel (bootstrap)
  * mpfr-devel (bootstrap)
  * libmpc-devel (bootstrap)
  * zip
  * perl
  * dejagnu (testing)
  * expect (testing)
  * tcl (testing)

```
wget ftp://ftp.mpi-sb.mpg.de/pub/gnu/mirror/gcc.gnu.org/pub/gcc/releases/gcc-6.4.0/gcc-6.4.0.tar.gz
tar xf gcc-6.4.0.tar.gz
cd gcc-6.4.0
mkdir build
cd build
../configure --disable-multilib --prefix=<install-dst>
make
# Testing
make -k check
make install
```

*Do not put gcc + libs on PATH* (unless you know what you are doing).

## Installing cmake-3.7.2

Requirements on CentOS 7:

  * wget
  * make
  * gcc
  * gcc-c++
  * ncurses-devel (optional, for ccmake)

```
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar xf cmake-3.7.2.tar.gz
cd cmake-3.7.2.tar.gz
./bootstrap --prefix<install-dst>
make
make install
```

Put cmake on PATH (if appropriate)

**Fix the bug in CpackRPM**

`"<path-to-cmake>/share/cmake-3.7/Modules/CPackRPM.cmake" line 2273 of 2442`

The line

```
set(RPMBUILD_FLAGS "-bb")
```
needs to be before

```
if(CPACK_RPM_GENERATE_USER_BINARY_SPECFILE_TEMPLATE OR NOT CPACK_RPM_USER_BINARY_SPECFILE)
```

It was probably accidentally placed after, and is fixed in later cmake
releases.

## Installing clang-3.9

Requirements on CentOS 7:

  * wget
  * make
  * cmake

```
wget http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz
tar xf llvm-3.9.1.src.tar.xz
mv llvm-3.9.1.src llvm

wget http://releases.llvm.org/3.9.1/cfe-3.9.1.src.tar.xz
tar xf cfe-3.9.1.src.tar.xz
mv cfe-3.9.1.src llvm/tools/clang

cd llvm
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE="Release" -DGCC_INSTALL_PREFIX=<gcc-dir> \
-DCMAKE_C_COMPILER=<gcc> -DCMAKE_CXX_COMPILER=<g++> \
-DCMAKE_CXX_LINK_FLAGS="-L<gcc-dir>/lib64 -Wl,-rpath,<gcc-dir>/lib64" \
-DCMAKE_INSTALL_PREFIX=<install-dst> ..
make
# Testing
make check-clang
make install
```

Put clang on PATH (if appropriate)

## Memgraph

Requirements on CentOS 7:

  * libuuid-devel (antlr4)
  * java-1.8.0-openjdk (antlr4)
  * boost-static (too low version --- compile manually)
  * rpm-build (RPM)
  * python3 (tests, ...)
  * which (required for rocksdb)
  * sbcl (lisp C++ preprocessing)

### Boost 1.62

```
wget https://netix.dl.sourceforge.net/project/boost/boost/1.62.0/boost_1_62_0.tar.gz
tar xf boost_1_62_0.tar.gz
cd boost_1_62_0
./bootstrap.sh --with-toolset=clang --with-libraries=iostreams,serialization --prefix=<install-dst>
./b2
# Default installs to /usr/local/
./b2 install
```

### Building Memgraph

clang is *required* to be findable by cmake, i.e. it should be on PATH.
cmake isn't required to be on the path, since you run it manually, so can use
the full path to executable in order to run it. Obviously, it is convenient to
put cmake also on PATH.

Building is done as explained in [Quick Start](quick-start.md), but each
`make` invocation needs to be prepended with:

`LD_RUN_PATH=<gcc-dir>/lib64 make ...`

### RPM

Name format: `memgraph-<version>-<pkg-version>.<arch>.rpm`
