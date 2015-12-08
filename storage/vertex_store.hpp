#pragma once

#include "vertex.hpp"
#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "data_structures/skiplist/skiplist.hpp"
#include "utils/counters/atomic_counter.hpp"

class VertexStore
{
public:
    Vertex* insert(tx::Transaction& transaction)
    {
        // get next vertex id
        auto next = counter.next(std::memory_order_acquire);

        // create new vertex record
        VertexRecord vertex_record;

        // insert the new vertex record into the vertex store
        auto store_accessor = vertex_store.access();
        auto result = store_accessor.insert_unique(next, std::move(vertex_record)); 

        // create new vertex
        auto inserted_vertex_record = result.first;
        auto vertex_accessor = inserted_vertex_record->second.access(transaction);
        auto vertex = vertex_accessor.insert();

        return vertex;
    }

    const Vertex* find(tx::Transaction& transaction, const Id& id)
    {
        // find vertex record
        auto store_accessor = vertex_store.access();
        auto vertex_record = store_accessor.find(id);

        if (vertex_record == store_accessor.end())
            return nullptr;

        // find vertex
        auto vertex_accessor = vertex_record->second.access(transaction);
        auto vertex = vertex_accessor.find();

        return vertex;
    }

private:
    SkipList<uint64_t, VertexRecord> vertex_store;
    AtomicCounter<uint64_t> counter;
};

