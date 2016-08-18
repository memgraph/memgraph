#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/indexes/index_record.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/property_family.hpp"
#include "transactions/transaction.hpp"

template <class T, class Derived, class vlist_t = mvcc::VersionList<T>>
class RecordAccessor
{
    friend DbAccessor;

public:
    RecordAccessor(vlist_t *vlist, DbTransaction &db) : vlist(vlist), db(db)
    {
        assert(vlist != nullptr);
    }

    RecordAccessor(T *t, vlist_t *vlist, DbTransaction &db)
        : record(t), vlist(vlist), db(db)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
    }

    RecordAccessor(RecordAccessor const &other) = default;
    RecordAccessor(RecordAccessor &&other) = default;

    bool empty() const { return record == nullptr; }

    // Fills accessor and returns true if there is valid data for current
    // transaction false otherwise.
    bool fill() const
    {
        const_cast<RecordAccessor *>(this)->record = vlist->find(db.trans);
        return record != nullptr;
    }

    const Id &id() const
    {
        assert(!empty());
        return vlist->id;
    }

    Derived update() const
    {
        assert(!empty());

        return Derived(vlist->update(db.trans), vlist, db);
    }

    bool remove() const
    {
        assert(!empty());

        return vlist->remove(record, db.trans);
    }

    const Property &at(prop_key_t &key) const { return properties().at(key); }

    template <class V>
    auto at(type_key_t<V> &key) const;

    template <class V, class... Args>
    void set(type_key_t<V> &key, Args &&... args)
    {
        properties().template set<V>(key, std::forward<Args>(args)...);
    }

    void set(prop_key_t &key, Property::sptr value)
    {
        properties().set(key, std::move(value));
    }

    void clear(prop_key_t &key) { properties().clear(key); }

    template <class Handler>
    void accept(Handler &handler) const
    {
        properties().template accept<Handler>(handler);
    }

    Properties &properties() const { return record->data.props; }

    explicit operator bool() const { return record != nullptr; }

    T const *operator->() const { return record; }
    T *operator->() { return record; }

    // Assumes same transaction
    friend bool operator==(const RecordAccessor &a, const RecordAccessor &b)
    {
        return a.vlist == b.vlist;
    }

    // Assumes same transaction
    friend bool operator!=(const RecordAccessor &a, const RecordAccessor &b)
    {
        return !(a == b);
    }

protected:
    IndexRecord<T, std::nullptr_t> create_index_record()
    {
        return create_index_record(std::nullptr_t());
    }

    template <class K>
    IndexRecord<T, K> create_index_record(K &&key)
    {
        return IndexRecord<T, K>(std::move(key), record, vlist);
    }

    T *record{nullptr};
    vlist_t *const vlist;
    DbTransaction &db;
};
