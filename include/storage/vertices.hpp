#pragma once

#include <string>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "utils/counters/atomic_counter.hpp"
#include "utils/option.hpp"

#include "storage/model/properties/property_family.hpp"
#include "storage/vertex_record.hpp"

class DbTransaction;
class VertexAccessor;

using VertexPropertyFamily = PropertyFamily<TypeGroupVertex>;
template <class K>
using VertexIndexBase = IndexBase<TypeGroupVertex, K>;

class Vertices
{
public:
    using vertices_t = ConcurrentMap<uint64_t, VertexRecord>;
    using prop_familys_t =
        ConcurrentMap<std::string, std::unique_ptr<VertexPropertyFamily>>;

    vertices_t::Accessor access();

    Option<const VertexAccessor> find(DbTransaction &t, const Id &id);

    // Creates new Vertex and returns filled VertexAccessor.
    VertexAccessor insert(DbTransaction &t);

    VertexPropertyFamily &
    property_family_find_or_create(const std::string &name);

private:
    vertices_t vertices;
    // TODO: Because families wont be removed this could be done with more
    // efficent
    // data structure.
    prop_familys_t prop_familys;
    AtomicCounter<uint64_t> counter;
};
