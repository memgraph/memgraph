#pragma once

// #include "storage/indexes/index_record.hpp"
#include <functional>
#include <memory>
#include "utils/border.hpp"
#include "utils/iterator/iterator_base.hpp"

class DbTransaction;
class DbAccessor;

template <class T, class K>
class IndexRecord;

// Defines ordering of data
enum Order
{
    None = 0,
    Ascending = 1,
    Descending = 2,
};

// Interface for all indexes.
// T type of record.
// K type of key on which records are ordered
template <class T, class K>
class IndexBase
{
public:
    typedef T value_type;
    typedef K key_type;

    IndexBase(bool unique, Order order) : unique(unique), order(order) {}

    // Insert's value.
    // unique => returns false if there is already valid equal value.
    // nonunique => always succeds.
    virtual bool insert(IndexRecord<T, K> &&value) = 0;

    // Returns iterator which returns valid records in range.
    // order==noe => doesn't guarantee any order of returned records.
    // order==Ascending => guarantees order of returnd records will be from
    // smallest to largest.
    // order==Descending => guarantees order of returned records will be from
    // largest to smallest.
    // Range must be from<=to
    virtual std::unique_ptr<IteratorBase<const typename T::Accessor>>
    for_range(DbAccessor &, Border<K> from = Border<K>(),
              Border<K> to = Border<K>()) = 0;

    // Removes for all transactions obsolete Records.
    // Cleaner has to call this method when he decideds that it is time for
    // cleaning.
    virtual void clean(DbTransaction &) = 0;

    // Are the records unique
    const bool unique;
    // Ordering of the records.
    const Order order;
};
