#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/properties.hpp"

template <class T, class Store, class Derived>
class RecordAccessor
{
protected:
    using vlist_t = mvcc::VersionList<T>;

public:
    RecordAccessor() = default;

    RecordAccessor(T* record, vlist_t* vlist, Store* store)
        : record(record), vlist(vlist), store(store)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
        assert(store != nullptr);
    }

    bool empty() const
    {
        return record == nullptr;
    }

    const Id& id() const
    {
        assert(!empty());
        return vlist->id();
    }

    Derived update(tx::Transaction& t) const
    {
        assert(!empty());

        auto accessor = vlist->access(t);
        return Derived(accessor.update(t), vlist, store);
    }

    bool remove(tx::Transaction& t) const
    {
        assert(!empty());

        auto accessor = vlist->access(t);
        return accessor.remove(record);
    }

    const Property& property(const std::string& key) const
    {
        return record->data.props.at(key);
    }

    template <class V, class... Args>
    void property(const std::string& key, Args&&... args)
    {
        record->data.props.template set<V>(key, std::forward<Args>(args)...);
    }

    void property(const std::string& key, Property::sptr value)
    {
        record->data.props.set(key, std::move(value));
    }

    Properties& properties() const
    {
        return record->data.props;
    }

    explicit operator bool() const
    {
        return record != nullptr;
    }

// protected:
    T* const record {nullptr};
    vlist_t* const vlist {nullptr};
    Store* const store {nullptr};
};
