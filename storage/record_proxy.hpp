#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"

template <typename T, typename Store, typename Derived>
class RecordProxy
{
public:
    RecordProxy(T* record, Store *store, mvcc::VersionList<T> *version_list) :
        record(record), store(store), version_list(version_list)
    {
    }

    RecordProxy(const RecordProxy& record_proxy) = delete;

    RecordProxy(RecordProxy&& other) :
        record(other.record), store(other.store), version_list(other.version_list)
    {
        other.record = nullptr;
        other.store = nullptr;
        other.version_list = nullptr;
    }

    ~RecordProxy()
    {
        // TODO: implementation
    }

    Derived update(tx::Transaction& transaction) const
    {
        // TODO: implementation
        transaction.commit();
        return nullptr;
    }

    void remove(tx::Transaction& transaction) const
    {
        // TODO: implementation
        transaction.commit();
    }

    template<typename K>
    Property* property(const K& key) const
    {
        return record->data.props.find(key);
    }
   
    template<typename K, typename V> 
    void property(const K& key, const V& value)
    {
        record->data.props.template set<String>(key, value);
        // TODO: update the index
    }

    T* version()
    {
        return record;
    }

private:
    T* record;
    Store *store;
    mvcc::VersionList<T> *version_list;
};
