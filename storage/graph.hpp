#pragma once

#include "storage/vertex_store.hpp"
#include "data_structures/skiplist/skiplist.hpp"
#include "edge.hpp"

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

    VertexStore vertex_store;
    EdgeStore edges;
};
