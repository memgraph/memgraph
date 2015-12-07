#pragma once

#include <list>

#include "mvcc/atom.hpp"
#include "mvcc/store.hpp"
#include "mvcc/mvcc_error.hpp"
#include "mvcc/version_list.hpp"
#include "data_structures/skiplist/skiplist.hpp"

#include "vertex.hpp"
#include "edge.hpp"

using VertexStore = SkipList<uint64_t, VertexRecord::uptr>;
using EdgeStore = SkipList<uint64_t, EdgeRecord::uptr>;

class Graph
{
public:
    Graph() {}

    // TODO: connect method
    // procedure:
    //     1. acquire unique over first node
    //     2. acquire unique over second node
    //     3. add edge
    
    // TODO: find vertex by Id

    VertexStore vertices;
    EdgeStore edges;
};
