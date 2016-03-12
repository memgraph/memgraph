#pragma once

#include "data_structures/skiplist/skiplist.hpp"
#include "storage/vertices.hpp"
#include "storage/edges.hpp"

class Graph
{
public:
    Graph() {}

    Vertices vertices;
    Edges edges;
};
