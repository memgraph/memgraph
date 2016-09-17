#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/indexes/index_record.hpp"
#include "storage/indexes/index_update.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/property_family.hpp"
#include "storage/model/properties/stored_property.hpp"
#include "transactions/transaction.hpp"

template <class TG, class Derived>
class RecordAccessor
{
    friend DbAccessor;
    using vlist_t = typename TG::vlist_t;
    using T = typename TG::record_t;

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

    RecordAccessor(RecordAccessor const &other)
        : record(other.record), vlist(other.vlist), db(other.db)
    {
    }
    RecordAccessor(RecordAccessor &&other)
        : record(other.record), vlist(other.vlist), db(other.db)
    {
    }

    bool empty() const { return record == nullptr; }

    // Fills accessor and returns true if there is valid data for current
    // transaction false otherwise.
    bool fill() const
    {
        const_cast<RecordAccessor *>(this)->record = vlist->find(db.trans);
        return record != nullptr;
    }

    const Id &id() const { return vlist->id; }

    // True if record visible for current transaction is visible to given
    // transaction id.
    bool is_visble_to(tx::TransactionRead const &id)
    {
        return record->visible(id);
    }

    // Returns new IndexRecord with given key.
    template <class K>
    IndexRecord<TG, K> create_index_record(K &&key)
    {
        return IndexRecord<TG, K>(std::move(key), record, vlist);
    }

    // TODO: Test this
    Derived update() const
    {
        assert(!empty());

        if (record->is_visible_write(db.trans)) {
            // TODO: VALIDATE THIS BRANCH. THEN ONLY THIS TRANSACTION CAN SEE
            // THIS DATA WHICH MEANS THAT IT CAN CHANGE IT.
            return Derived(record, vlist, db);

        } else {
            auto new_record = vlist->update(db.trans);

            // TODO: Validate that update of record in this accessor is correct.
            const_cast<RecordAccessor *>(this)->record = new_record;

            // Add record to update index.
            db.to_update_index<TG>(vlist, new_record);

            return Derived(new_record, vlist, db);
        }
    }

    const StoredProperty<TG> &at(PropertyFamily<TG> &key) const
    {
        return properties().at(key);
    }

    const StoredProperty<TG> &at(property_key<TG> &key) const
    {
        return properties().at(key);
    }

    template <class V>
    OptionPtr<const V> at(type_key_t<TG, V> &key) const
    {
        return properties().template at<V>(key);
    }

    void set(property_key<TG> &key, Property value)
    {
        properties().set(StoredProperty<TG>(std::move(value), key));
    }

    void set(StoredProperty<TG> value) { properties().set(std::move(value)); }

    void clear(property_key<TG> &key) { properties().clear(key); }

    void clear(PropertyFamily<TG> &key) { properties().clear(key); }

    template <class Handler>
    void accept(Handler &handler) const
    {
        properties().template accept<Handler>(handler);
    }

    template <class Handler>
    void handle(Handler &handler) const
    {
        properties().template handle<Handler>(handler);
    }

    Properties<TG> &properties() const { return record->data.props; }

    explicit operator bool() const { return record != nullptr; }

    T const *operator->() const { return record; }
    T *operator->() { return record; }

    RecordAccessor &operator=(const RecordAccessor &other)
    {
        record = other.record;
        vlist_t *&vl = const_cast<vlist_t *&>(vlist);
        vl = other.vlist;
        return *this;
    }

    RecordAccessor &operator=(RecordAccessor &&other)
    {
        record = other.record;
        vlist_t *&vl = const_cast<vlist_t *&>(vlist);
        vl = other.vlist;
        return *this;
    }

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
    void remove() const;

    T *record{nullptr};
    vlist_t *const vlist;
    DbTransaction &db;
};
