#pragma once

#include "common.hpp"
#include "storage/vertex_accessor.hpp"

class Vertices
{
public:
    Vertex::Accessor find(tx::Transaction& t, const Id& id)
    {
        auto vertices_accessor = vertices.access();
        auto vertices_iterator = vertices_accessor.find(id);

        if (vertices_iterator == vertices_accessor.end())
            return Vertex::Accessor();

        // find vertex
        auto versions_accessor = vertices_iterator->second.access(t);
        auto vertex = versions_accessor.find();

        return Vertex::Accessor(vertex, &vertices_iterator->second, this);
    }

    Vertex::Accessor insert(tx::Transaction& t)
    {
        // get next vertex id
        auto next = counter.next(std::memory_order_acquire);

        // create new vertex record
        VertexRecord vertex_record;
        vertex_record.id(next);

        // insert the new vertex record into the vertex store
        auto vertices_accessor = vertices.access();
        auto result = vertices_accessor.insert_unique(
            next,
            std::move(vertex_record)
        );

        // create new vertex
        auto inserted_vertex_record = result.first;
        auto vertex_accessor = inserted_vertex_record->second.access(t);
        auto vertex = vertex_accessor.insert();

        return Vertex::Accessor(vertex, &inserted_vertex_record->second, this);
    }

private:
    // Indexes indexes;

    SkipList<uint64_t, VertexRecord> vertices;
    AtomicCounter<uint64_t> counter;
};
