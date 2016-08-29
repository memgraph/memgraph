#include "storage/label/label_store.hpp"

LabelStore::store_t::Accessor LabelStore::access() { return labels.access(); }

const Label &LabelStore::find_or_create(const char *name)
{
    auto accessor = labels.access();
    auto it = accessor.find(CharStr(name));
    if (it == accessor.end()) {
        auto l = std::make_unique<Label>(name);
        auto k = l->char_str();
        it = accessor.insert(k, std::move(l)).first;
    }
    return *(it->second);
}

bool LabelStore::contains(const char *name) // const
{
    auto accessor = labels.access();
    return accessor.find(CharStr(name)) != accessor.end();
}
