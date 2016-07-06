#pragma once

#include <stdexcept>

#include "storage/edge_type/edge_type.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"

class EdgeTypeStore
{
public:

    const EdgeType& find_or_create(const std::string& name);

    bool contains(const std::string& name); // TODO: const

    // TODO: implement find method
    //       return { EdgeType, is_found }
    
    // TODO: find by reference if it is possible (should be faster)
    //       figure out the fastest way to store and find types
    //       do the same for labels
    
    // TODO: EdgeTypeStore and LabelStore are almost the same
    //       templetize the two of them

private:
    ConcurrentSet<EdgeType> edge_types;
};
