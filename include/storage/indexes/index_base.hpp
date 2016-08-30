#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include "mvcc/id.hpp"

// #include "storage/indexes/index_record.hpp"
#include "storage/garbage/delete_sensitive.hpp"
#include "utils/border.hpp"
#include "utils/iterator/iterator_base.hpp"
#include "utils/order.hpp"

template <class TG, class K>
class IndexRecord;

class DbTransaction;
class DbAccessor;
namespace tx
{
class Transaction;
}

// Interface for all indexes.
// TG type group
// K type of key on which records are ordered
template <class TG, class K>
class IndexBase : public DeleteSensitive
{
public:
    // typedef T value_type;
    // typedef K key_type;

    // Created with the database
    IndexBase(bool unique, Order order);

    IndexBase(bool unique, Order order, const tx::Transaction &t);

    virtual ~IndexBase(){};

    // Insert's value.
    // unique => returns false if there is already valid equal value.
    // nonunique => always succeds.
    virtual bool insert(IndexRecord<TG, K> &&value) = 0;

    // Returns iterator which returns valid filled records in range.
    // order==noe => doesn't guarantee any order of returned records.
    // order==Ascending => guarantees order of returnd records will be from
    // smallest to largest.
    // order==Descending => guarantees order of returned records will be from
    // largest to smallest.
    // Range must be from<=to
    virtual std::unique_ptr<IteratorBase<const typename TG::accessor_t>>
    for_range(DbAccessor &, Border<K> from = Border<K>(),
              Border<K> to = Border<K>()) = 0;

    // Removes for all transactions obsolete Records.
    // Cleaner has to call this method when he decideds that it is time for
    // cleaning. Id must be id of oldest active transaction.
    virtual void clean(const Id &id) = 0;

    // Activates index for readers.
    void activate();

    // True if index is ready for reading.
    bool can_read();

    // True if transaction is obliged to insert T into index.
    bool is_obliged_to_insert(const tx::Transaction &t);

    bool unique() { return _unique; }

    Order order() { return _order; }

private:
    // Are the records unique
    const bool _unique;
    // Ordering of the records.
    const Order _order;
    // Id of transaction which created this index.
    const Id created;
    // Active state
    std::atomic_bool active = {false};
};
