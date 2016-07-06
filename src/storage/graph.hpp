#pragma once

#include "storage/edges.hpp"
#include "storage/edge_type/edge_type_store.hpp"
#include "storage/label/label_store.hpp"
#include "storage/vertices.hpp"

class Graph
{
public:
    Graph() {}

    Edges edges;
    Vertices vertices;

    LabelStore label_store;
    EdgeTypeStore edge_type_store;
};
