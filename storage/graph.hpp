#ifndef MEMGRAPH_STORAGE_GRAPH_HPP
#define MEMGRAPH_STORAGE_GRAPH_HPP

#include <list>

#include "mvcc/atom.hpp"
#include "mvcc/store.hpp"

#include "vertex.hpp"
#include "edge.hpp"

using VertexStore = mvcc::MvccStore<Vertex>;
using EdgeStore = mvcc::MvccStore<Edge>;

class Graph
{
public:
    Graph() {}

    EdgeStore::iterator connect(Vertex a, Vertex b, const Transaction& t)
    {
         auto it = edges.insert(t);

         it->
         
         return it;
    }

    VertexStore vertices;
    EdgeStore edges;
};

#endif
