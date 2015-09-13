#ifndef MEMGRAPH_STORAGE_EDGE_HPP
#define MEMGRAPH_STORAGE_EDGE_HPP

#include <vector>

#include "model/record.hpp"

struct Vertex;

struct Edge : public Record<Edge>
{
    Vertex* from;
    Vertex* to;
};

#endif
