#pragma once

#include <string>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "utils/counters/atomic_counter.hpp"
#include "utils/option.hpp"

#include "storage/edge_record.hpp"
#include "storage/model/properties/property_family.hpp"

class EdgeAccessor;
class DbTransaction;

using EdgePropertyFamily = PropertyFamily<TypeGroupEdge>;
template <class K>
using EdgeIndexBase = IndexBase<TypeGroupEdge, K>;

class Edges
{
    using prop_familys_t = ConcurrentMap<std::string, EdgePropertyFamily *>;

public:
    Option<const EdgeAccessor> find(DbTransaction &t, const Id &id);

    // Creates new Edge and returns filled EdgeAccessor.
    EdgeAccessor insert(DbTransaction &t, VertexRecord *from, VertexRecord *to);

    EdgePropertyFamily &property_family_find_or_create(const std::string &name);

private:
    ConcurrentMap<uint64_t, EdgeRecord> edges;
    // TODO: Because familys wont be removed this could be done with more
    // efficent
    // data structure.
    prop_familys_t prop_familys;
    AtomicCounter<uint64_t> counter;
};
