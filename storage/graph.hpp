#ifndef MEMGRAPH_STORAGE_GRAPH_HPP
#define MEMGRAPH_STORAGE_GRAPH_HPP

#include <list>

#include "mvcc/atom.hpp"
#include "mvcc/store.hpp"
#include "mvcc/mvcc_error.hpp"

#include "vertex.hpp"
#include "edge.hpp"

using VertexStore = mvcc::MvccStore<Vertex>;
using EdgeStore = mvcc::MvccStore<Edge>;

class Graph
{
public:
    Graph() {}

    mvcc::Atom<Edge>* connect(mvcc::Atom<Vertex>& atom_a, Vertex& a,
                              mvcc::Atom<Vertex>& atom_b, Vertex& b,
                              const tx::Transaction& t)
    {
        // try to lock A
        auto guard_a = atom_a.acquire_unique();

        if(a.tx.max())
            throw mvcc::MvccError("can't serialize due to\
                concurrent operation(s)");

        auto guard_b = atom_b.acquire_unique();

        if(b.tx.max())
            throw mvcc::MvccError("can't serialize due to\
                concurrent operation(s)");

         return edges.insert(t);
    }

    VertexStore vertices;
    EdgeStore edges;
};

#endif
