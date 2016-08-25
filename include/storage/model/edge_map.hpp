#pragma once

#include "data_structures/map/rh_hashmultimap.hpp"
#include "storage/edge_record.hpp"
// #include "storage/vertex_record.hpp"

class EdgeMap
{
public:
    auto begin() { return edges.begin(); }
    auto begin() const { return edges.begin(); }
    auto cbegin() const { return edges.begin(); }

    auto end() { return edges.end(); }
    auto end() const { return edges.end(); }
    auto cend() const { return edges.end(); }

    size_t degree() const { return edges.size(); }

    void add(EdgeRecord *edge) { edges.add(edge); }

    void remove(EdgeRecord *edge)
    {
        edges.remove(edge); // Currently the return is ignored
    }

    bool contains(VertexRecord *vr) const { return edges.contains(vr); }

    void clear() { edges.clear(); }

private:
    RhHashMultiMap<VertexRecord *, EdgeRecord> edges;
};
