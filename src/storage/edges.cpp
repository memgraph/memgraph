#include "storage/edges.hpp"

#include "storage/edge_accessor.hpp"
#include "utils/iterator/iterator.hpp"

Option<const EdgeAccessor> Edges::find(DbTransaction &t, const Id &id)
{
    auto edges_accessor = edges.access();
    auto edges_iterator = edges_accessor.find(id);

    if (edges_iterator == edges_accessor.end())
        return make_option<const EdgeAccessor>();

    return make_option_const(EdgeAccessor(&edges_iterator->second, t));
}

EdgeAccessor Edges::insert(DbTransaction &t, VertexRecord *from,
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
    auto edge = inserted_edge_record->second.insert(t.trans);
    t.to_update_index<TypeGroupEdge>(&inserted_edge_record->second, edge);

    return EdgeAccessor(edge, &inserted_edge_record->second, t);
}

EdgePropertyFamily &
Edges::property_family_find_or_create(const std::string &name)
{
    auto acc = prop_familys.access();
    auto it = acc.find(name);
    if (it == acc.end()) {
        EdgePropertyFamily *family = new EdgePropertyFamily(name);
        auto res = acc.insert(name, family);
        if (!res.second) {
            delete family;
        }
        it = res.first;
    }
    return *(it->second);
}
