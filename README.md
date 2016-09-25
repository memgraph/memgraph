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

### Bolt

sudo apt-get install libssl-dev

## build
```
cd build
cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DRUNTIME_ASSERT=OFF -DTHROW_EXCEPTION_ON_ERROR=OFF -DNDEBUG=OFF ..

# Flags:
#   -DCMAKE_C_COMPILER=clang
#   -DCMAKE_CXX_COMPILER=clang++
#   -DRUNTIME_ASSERT=OFF
#   -DTHROW_EXCEPTION_ON_ERROR=OFF
#   -DNDEBUG=ON
#   -DCMAKE_BUILD_TYPE:STRING=debug
#   -DCMAKE_BUILD_TYPE:STRING=release

make
ctest
ctest -V
ctest -R test_name
ctest -R unit
ctest -R concurrent
ctest -R concurrent --parallel 4
```

## Proof of concept

### Dressipi

Run command:
```
cd build/poc
MEMGRAPH_CONFIG=/path/to/config/memgraph.yaml ./poc_astar -v "dressipi/graph_nodes_export.csv" -e "dressipi/graph_edges_export.csv"
```

### Powerlinx

Run command:
```
cd build/poc
MEMGRAPH_CONFIG=/path/to/config/memgraph.yaml ./profile -d ";" -v "node_account.csv" -v "node_company.csv" -v "node_opportunity.csv"  -v "node_personnel.csv" -e "rel_created.csv" -e "rel_has_match.csv" -e "rel_interested_in.csv" -e "rel_is_employee.csv" -e "rel_partnered_with.csv" -e "rel_reached_to.csv" -e "rel_searched_and_clicked.csv" -e "rel_viewed.csv" -e "rel_works_in.csv"
```

# Custom test build example
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
