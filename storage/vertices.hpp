#pragma once

#include "vertex.hpp"
#include "common.hpp"
#include "vertex_proxy.hpp"

class Vertices
{
public:
    const Vertex* find(tx::Transaction& transaction, const Id& id)
    {
        // find vertex record
        auto vertices_accessor = vertices.access();
        auto vertex_record = vertices_accessor.find(id);

        if (vertex_record == vertices_accessor.end())
            return nullptr;

        // find vertex
        auto vertex_accessor = vertex_record->second.access(transaction);
        auto vertex = vertex_accessor.find();

        // TODO: here is problem to create vertex_proxy because vertex
        // is const pointer

        return vertex;
    }

private:
    Indexes indexes;

    SkipList<uint64_t, VersionList<Vertex>> vertices;
    AtomicCounter<uint64_t> counter;
};
