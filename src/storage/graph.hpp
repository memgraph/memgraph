#pragma once

#include "storage/vertices.hpp"
#include "storage/edges.hpp"
#include "storage/label_store.hpp"

class Graph
{
public:
    Graph() {}

    Edges edges;
    Vertices vertices;
    LabelStore label_store;
};
