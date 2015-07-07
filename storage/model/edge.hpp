#ifndef MEMGRAPH_DATA_MODEL_EDGE_HPP
#define MEMGRAPH_DATA_MODEL_EDGE_HPP

#include <vector>

#include "record.hpp"

template <class id_t, class lock_t>
struct Vertex;

template <class id_t,
          class lock_t>
struct Edge : public Record<Edge<id_t, lock_t>, id_t, lock_t>
{
    Edge(uint64_t id) : Record<Edge<id_t, lock_t>, id_t, lock_t>(id) {}

    using vertex_t = Vertex<id_t, lock_t>;

    // pointer to the vertex this edge points to
    vertex_t* to;
};

#endif
