#pragma once

#include "data_structures/skiplist/skiplist.hpp"
#include "storage/vertices.hpp"
#include "storage/edges.hpp"

class Graph
{
public:
    Graph() {}

    // TODO: connect method
    // procedure:
    //     1. acquire unique over first node
    //     2. acquire unique over second node
    //     3. add edge

    Vertices vertices;
    Edges edges;
};
