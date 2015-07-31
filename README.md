# memgraph

Memgraph is an ACID compliant high performance transactional distributed
in-memory graph database featuring runtime native query compiling, lock free
data structures, multi-version concurrency control and asynchronous IO.

## dependencies

Memgraph can be compiled using any modern c++ compiler. It mostly relies on
the standard template library, however, some things do require external
libraries.

Some code contains linux-specific libraries and the build is only supported
on a 64 bit linux kernel.

* linux
* clang 3.5 or Gcc 4.8 (good c++11 support, especially lock free atomics)
* boost 1.55 (or something, probably works with almost anything)
* lexertl (2015-07-14)
* catch (for compiling tests)

## build

