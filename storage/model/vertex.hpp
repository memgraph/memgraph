#ifndef MEMGRAPH_STORAGE_MODEL_VERTEX_HPP
#define MEMGRAPH_STORAGE_MODEL_VERTEX_HPP

#include <vector>

#include "record.hpp"
#include "edge.hpp"

template <class id_t,
          class lock_t>
struct Vertex : public Record<Vertex<id_t, lock_t>, id_t, lock_t>
{
    Vertex(uint64_t id) : Record<Vertex<id_t, lock_t>, id_t, lock_t>(id) {}

    using edge_t = Edge<id_t, lock_t>;

    // adjacency list containing pointers to outgoing edges from this vertex
    std::vector<edge_t*> out;
};

#endif
