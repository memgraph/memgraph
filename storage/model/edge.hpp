#ifndef MEMGRAPH_DATA_MODEL_EDGE_HPP
#define MEMGRAPH_DATA_MODEL_EDGE_HPP

#include <vector>

#include "record.hpp"

struct Vertex;

struct 

struct Edge : public Record<Edge>
{
    Vertex* from;
    Vertex* to;
};

#endif
