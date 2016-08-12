#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
#include "transactions/transaction.hpp"

template <class T, class Derived, class vlist_t = mvcc::VersionList<T>>
class RecordAccessor
{
public:
    RecordAccessor(DbTransaction &db) : db(db){};

    RecordAccessor(T *t, vlist_t *vlist, DbTransaction &db)
        : record(t), vlist(vlist), db(db)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
    }

    RecordAccessor(vlist_t *vlist, DbTransaction &db)
        : record(vlist->find(db.trans)), vlist(vlist), db(db)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
    }

    bool empty() const { return record == nullptr; }

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

    const Property &property(const std::string &key) const
    {
        return record->data.props.at(key);
    }

    template <class V, class... Args>
    void property(const std::string &key, Args &&... args)
    {
        record->data.props.template set<V>(key, std::forward<Args>(args)...);
    }

    void property(const std::string &key, Property::sptr value)
    {
        record->data.props.set(key, std::move(value));
    }

    Properties &properties() const { return record->data.props; }

    explicit operator bool() const { return record != nullptr; }

    // protected:
    T *const record{nullptr};
    vlist_t *const vlist{nullptr};
    DbTransaction &db;
};
