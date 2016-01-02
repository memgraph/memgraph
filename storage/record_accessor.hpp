#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"

template <class T, class Store, class Derived>
class RecordAccessor
{
protected:
    using vlist_t = mvcc::VersionList<T>;

public:
    RecordAccessor() = default;

    RecordAccessor(T* record, vlist_t* vlist, Store* store)
        : record(record), vlist(vlist), store(store) {}

    const Id& id() const
    {
        auto accessor = vlist->access();
        return accessor.id();
    }

    bool empty() const
    {
        return record == nullptr;
    }

    Derived update(tx::Transaction& t) const
    {
        assert(!empty());

        auto accessor = vlist->access();
        return Derived(accessor->update(t), vlist, store);
    }

    bool remove(tx::Transaction& t) const
    {
        assert(!empty());

        auto accessor = vlist->access();
        return accessor->remove(t);
    }

    const Property* property(const std::string& key) const
    {
        return record->props.at(key);
    }

    template <class V, class... Args>
    void property(const std::string& key, Args&&... args)
    {
        record->props.template set<V>(key, std::forward<Args>(args)...);
    }

protected:
    T* const record {nullptr};
    vlist_t* const vlist {nullptr};
    Store* const store {nullptr};
};
