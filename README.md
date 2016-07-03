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
* lemon (parser generator)
* catch (for compiling tests)

## build
```
cd build
cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DRUNTIME_ASSERT=OFF -DTHROW_EXCEPTION_ON_ERROR=OFF -DNDEBUG=OFF ..
make
ctest
ctest -V
ctest -R test_name
ctest -R unit
ctest -R concurrent
ctest -R concurrent --parallel 4
```

# custom test build example
clang++ -std=c++1y -o concurrent_skiplist ../tests/concurrent/skiplist.cpp -pg -I../src/ -I../libs/fmt -Lfmt-prefix/src/fmt-build/fmt -lfmt -lpthread

# TODO
* implement basic subset of queries:
    * create node
    * create edge
    * find node
    * find edge
    * update node
    * update edge
    * delete node
    * delete edge

* implement index
    * label index
    * type index
    * property index

* from header only approach to .hpp + .cpp
    * query engine has to be statically linked with the rest of the code

* unit test of queries that are manually implemented
    * src/query_engine/main_queries.cpp -> tests/unit/manual_queries

* unit test of query_engine

* console with history
