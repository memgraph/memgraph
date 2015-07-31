#ifndef MEMGRAPH_STORAGE_MODEL_VERTEX_HPP
#define MEMGRAPH_STORAGE_MODEL_VERTEX_HPP

#include <vector>

#include "record.hpp"
#include "edge.hpp"

struct Vertex : public Record<Vertex>
{
    Vertex(uint64_t id) : Record<Vertex>(id) {}

    // adjacency list containing pointers to outgoing edges from this vertex
    std::vector<Edge*> out;
};

#endif
