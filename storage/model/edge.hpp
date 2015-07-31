#ifndef MEMGRAPH_DATA_MODEL_EDGE_HPP
#define MEMGRAPH_DATA_MODEL_EDGE_HPP

#include <vector>

#include "record.hpp"

struct Vertex;

struct Edge : public Record<Edge>
{
    Edge(uint64_t id) : Record<Edge>(id) {}

    // pointer to the vertex this edge points to
    Vertex* to;
};

#endif
