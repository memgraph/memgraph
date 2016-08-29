#pragma once

#include <stdexcept>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "utils/char_str.hpp"

class EdgeTypeStore
{
public:
    using store_t = ConcurrentMap<CharStr, std::unique_ptr<EdgeType>>;

    store_t::Accessor access();

    const EdgeType &find_or_create(const char *name);

    bool contains(const char *name); // TODO: const

    // TODO: implement find method
    //       return { EdgeType, is_found }

    // TODO: find by reference if it is possible (should be faster)
    //       figure out the fastest way to store and find types
    //       do the same for labels

    // TODO: EdgeTypeStore and LabelStore are almost the same
    //       templetize the two of them

private:
    store_t edge_types;
};
