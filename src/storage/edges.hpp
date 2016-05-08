#pragma once

#include "common.hpp"
#include "edge_accessor.hpp"

class Edges
{
public:
    Edge::Accessor find(tx::Transaction& t, const Id& id)
    {
        auto edges_accessor = edges.access();
        auto edges_iterator = edges_accessor.find(id);

        if (edges_iterator == edges_accessor.end())
            return Edge::Accessor();

        // find vertex
        auto versions_accessor = edges_iterator->second.access(t);
        auto edge = versions_accessor.find();

        if (edge == nullptr)
            return Edge::Accessor();

        return Edge::Accessor(edge, &edges_iterator->second, this);
    }

    Edge::Accessor insert(tx::Transaction& t)
    {
        // get next vertex id
        auto next = counter.next(std::memory_order_acquire);

        // create new vertex record
        EdgeRecord edge_record(next);

        // insert the new vertex record into the vertex store
        auto edges_accessor = edges.access();
        auto result = edges_accessor.insert_unique(
            next,
            std::move(edge_record)
        );

        // create new vertex
        auto inserted_edge_record = result.first;
        auto edge_accessor = inserted_edge_record->second.access(t);
        auto edge = edge_accessor.insert();

        return Edge::Accessor(edge, &inserted_edge_record->second, this);
    }

private:
    SkipList<uint64_t, EdgeRecord> edges;
    AtomicCounter<uint64_t> counter;
};
