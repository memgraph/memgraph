#include "storage/edge_type/edge_type_store.hpp"

EdgeTypeStore::store_t::Accessor EdgeTypeStore::access()
{
    return edge_types.access();
}

const EdgeType &EdgeTypeStore::find_or_create(const char *name)
{
    auto accessor = edge_types.access();
    auto it = accessor.find(CharStr(name));
    if (it == accessor.end()) {
        auto l = std::make_unique<EdgeType>(name);
        auto k = l->char_str();
        it = accessor.insert(k, std::move(l)).first;
    }
    return *(it->second);
}

bool EdgeTypeStore::contains(const char *name) // const
{
    auto accessor = edge_types.access();
    return accessor.find(CharStr(name)) != accessor.end();
}
