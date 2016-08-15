#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
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

    template <class V>
    auto at(const std::string &key) const
    {
        return properties().at(key).template as<V>().value_ref();
    }

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
    T *record{nullptr};
    vlist_t *const vlist;
    DbTransaction &db;
};
