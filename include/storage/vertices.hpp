#pragma once

#include <memory>
#include <string>
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/db_transaction.hpp"
#include "storage/common.hpp"
#include "storage/model/properties/property_family.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/option.hpp"

class DbTransaction;

class Vertices
{
public:
    using vertices_t = ConcurrentMap<uint64_t, VertexRecord>;
    using prop_familys_t =
        ConcurrentMap<std::string, std::unique_ptr<PropertyFamily>>;

    vertices_t::Accessor access();

    Option<const Vertex::Accessor> find(DbTransaction &t, const Id &id);

    // Creates new Vertex and returns filled Vertex::Accessor.
    Vertex::Accessor insert(DbTransaction &t);

    PropertyFamily &property_family_find_or_create(const std::string &name);

private:
    vertices_t vertices;
    // TODO: Because families wont be removed this could be done with more
    // efficent
    // data structure.
    prop_familys_t prop_familys;
    AtomicCounter<uint64_t> counter;
};
