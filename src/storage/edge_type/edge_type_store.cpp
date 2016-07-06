#include "storage/edge_type/edge_type_store.hpp"

const EdgeType& EdgeTypeStore::find_or_create(const std::string& name)
{
    auto accessor = edge_types.access();
    return accessor.insert(EdgeType(name)).first;
}

bool EdgeTypeStore::contains(const std::string& name) // const
{
    auto accessor = edge_types.access();
    return accessor.find(EdgeType(name)) != accessor.end();
}
