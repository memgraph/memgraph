#pragma once

#include <list>

#include "mvcc/atom.hpp"
#include "mvcc/store.hpp"
#include "mvcc/mvcc_error.hpp"

#include "vertex.hpp"
#include "edge.hpp"

using VertexStore = mvcc::MvccStore<Vertex>;
using EdgeStore = mvcc::MvccStore<Edge>;

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
