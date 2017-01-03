#pragma once

#include "storage/edges.hpp"
#include "storage/edge_type/edge_type_store.hpp"
#include "storage/label/label_store.hpp"
#include "storage/vertices.hpp"

/**
 * Graph storage. Contains vertices and edges, labels and edges.
 */
class Graph
{
public:
    /**
     * default constructor 
     *
     * At the beginning the graph is empty.
     */
    Graph() = default;

    /** storage for all vertices related to this graph */
    Vertices vertices;

    /** storage for all edges related to this graph */
    Edges edges;

    /** storage for all labels */
    LabelStore label_store;

    /** storage for all types related for this graph */
    EdgeTypeStore edge_type_store;
};
