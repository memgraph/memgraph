#include "storage/label_store.hpp"

const Label& LabelStore::find_or_create(const std::string& name)
{
    auto accessor = labels.access();
    return accessor.insert(Label(name)).first;
}

bool LabelStore::contains(const std::string& name) // const
{
    auto accessor = labels.access();
    return accessor.find(Label(name)) != accessor.end();
}
