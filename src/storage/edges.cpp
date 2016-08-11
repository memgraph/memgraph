#include "storage/edges.hpp"

Edge::Accessor Edges::find(tx::Transaction &t, const Id &id)
{
    auto edges_accessor = edges.access();
    auto edges_iterator = edges_accessor.find(id);

    if (edges_iterator == edges_accessor.end()) return Edge::Accessor();

    // find edge
    auto edge = edges_iterator->second.find(t);

    if (edge == nullptr) return Edge::Accessor();

    return Edge::Accessor(edge, &edges_iterator->second, this);
}

Edge::Accessor Edges::insert(tx::Transaction &t, VertexRecord *from,
                             VertexRecord *to)
{
    // get next vertex id
    auto next = counter.next(std::memory_order_acquire);

    // create new vertex record
    EdgeRecord edge_record(next, from, to);

    // insert the new vertex record into the vertex store
    auto edges_accessor = edges.access();
    auto result = edges_accessor.insert(next, std::move(edge_record));

    // create new vertex
    auto inserted_edge_record = result.first;
    auto edge = inserted_edge_record->second.insert(t);

    return Edge::Accessor(edge, &inserted_edge_record->second, this);
}
