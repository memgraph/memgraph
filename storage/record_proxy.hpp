#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"

template <typename T, typename Store, typename Derived>
class RecordProxy
{
public:
    RecordProxy(
        const Id& id,
        T* version,
        Store *store,
        mvcc::VersionList<T> *version_list) :
        id(id), version(version), store(store), version_list(version_list)
    {
    }

    RecordProxy(const RecordProxy& record_proxy) = delete;

    RecordProxy(RecordProxy&& other) :
        id(other.id), version(other.version), store(other.store),
        version_list(other.version_list)
    {
        other.id = 0; // TODO: not very good idea because
        // replace with something else
        other.version = nullptr;
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
        return version->data.props.find(key);
    }
   
    template<typename K, typename V> 
    void property(const K& key, const V& value)
    {
        version->data.props.template set<String>(key, value);
        // TODO: update the index
    }

    Id record_id() const
    {
        return id;
    }

    T* record_version() const
    {
        return version;
    }

private:
    Id id;
    T* version;
    Store *store;
    mvcc::VersionList<T> *version_list;
};
