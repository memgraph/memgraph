#pragma once

#include "storage/indexes/index_base.hpp"

#include "data_structures/concurrent/concurrent_set.hpp"

// TODO: T shoud be TG (TypeGroup)
template <class T, class K>
class UniqueOrderedIndex : public IndexBase<T, K>
{
public:
    // typedef T value_type;
    // typedef K key_type;

    // Created with the database
    UniqueOrderedIndex(IndexLocation loc, Order order);

    UniqueOrderedIndex(IndexLocation loc, Order order,
                       tx::Transaction const &t);

    // Insert's value.
    // nonunique => always succeds.
    bool insert(IndexRecord<T, K> &&value) final;

    // Returns iterator which returns valid records in range.
    // ordered==None => doesn't guarantee any order of submitting records.
    iter::Virtual<const typename T::accessor_t>
    for_range(DbAccessor &t, Border<K> from = Border<K>(),
              Border<K> to = Border<K>()) final;

    // Same as for_range just whith known returned iterator.
    auto for_range_exact(DbAccessor &t, Border<K> from = Border<K>(),
                         Border<K> to = Border<K>());

    // Removes for all transactions obsolete Records.
    // Cleaner has to call this method when he decideds that it is time for
    // cleaning. Id must be id of oldest active transaction.
    void clean(const Id &id) final;

private:
    ConcurrentSet<IndexRecord<T, K>> set;
};
