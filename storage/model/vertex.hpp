#ifndef MEMGRAPH_STORAGE_MODEL_VERTEX_HPP
#define MEMGRAPH_STORAGE_MODEL_VERTEX_HPP

#include <vector>

#include "record.hpp"
#include "edge.hpp"

struct Vertex : public Record<Vertex>
{
    std::vector<Edge*> in;
    std::vector<Edge*> out;
};

#endif
