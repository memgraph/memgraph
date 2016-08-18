#pragma once

#include <string>
#include "data_structures/concurrent/concurrent_map.hpp"
#include "mvcc/version_list.hpp"
#include "storage/common.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

class Edges
{
    using prop_familys_t = ConcurrentMap<std::string, PropertyFamily *>;

public:
    Option<const Edge::Accessor> find(DbTransaction &t, const Id &id);

    // Creates new Edge and returns filled Edge::Accessor.
    Edge::Accessor insert(DbTransaction &t, VertexRecord *from,
                          VertexRecord *to);

    // auto property_family_access();

    PropertyFamily &property_family_find_or_create(const std::string &name);

private:
    ConcurrentMap<uint64_t, EdgeRecord> edges;
    // TODO: Because familys wont be removed this could be done with more
    // efficent
    // data structure.
    prop_familys_t prop_familys;
    AtomicCounter<uint64_t> counter;
};
