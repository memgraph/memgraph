#pragma once

#include "storage/indexes/index_base.hpp"
#include "storage/indexes/index_record.hpp"

#include "data_structures/concurrent/concurrent_list.hpp"

template <class T, class K>
class NonUniqueUnorderedIndex : public IndexBase<T, K>
{
public:
    typedef T value_type;
    typedef K key_type;

    NonUniqueUnorderedIndex();

    // Insert's value.
    // nonunique => always succeds.
    bool insert(IndexRecord<T, K> &&value) final;

    // Returns iterator which returns valid records in range.
    // ordered==None => doesn't guarantee any order of submitting records.
    std::unique_ptr<IteratorBase<const typename T::Accessor>>
    for_range(DbAccessor &t, Border<K> from = Border<K>(),
              Border<K> to = Border<K>()) final;

    // Same as for_range just whit known returned iterator.
    auto for_range_exact(DbAccessor &t, Border<K> from = Border<K>(),
                         Border<K> to = Border<K>());

    // Removes for all transactions obsolete Records.
    // Cleaner has to call this method when he decideds that it is time for
    // cleaning.
    void clean(DbTransaction &) final;

private:
    List<IndexRecord<T, K>> list;
};
