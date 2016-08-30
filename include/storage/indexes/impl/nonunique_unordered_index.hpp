#pragma once

#include "storage/indexes/index_base.hpp"
// #include "storage/indexes/index_record.hpp"

#include "data_structures/concurrent/concurrent_list.hpp"

template <class TG, class K>
class NonUniqueUnorderedIndex : public IndexBase<TG, K>
{
public:
    using store_t = List<IndexRecord<TG, K>>;
    // typedef T value_type;
    // typedef K key_type;

    // Created with the database
    NonUniqueUnorderedIndex();

    NonUniqueUnorderedIndex(tx::Transaction const &t);

    // Insert's value.
    // nonunique => always succeds.
    bool insert(IndexRecord<TG, K> &&value) final;

    // Returns iterator which returns valid records in range.
    // ordered==None => doesn't guarantee any order of submitting records.
    iter::Virtual<const typename TG::accessor_t>
    for_range(DbAccessor &t, Border<K> from = Border<K>(),
              Border<K> to = Border<K>()) final;

    // Same as for_range just whit known returned iterator.
    auto for_range_exact(DbAccessor &t, Border<K> from = Border<K>(),
                         Border<K> to = Border<K>());

    // Removes for all transactions obsolete Records.
    // Cleaner has to call this method when he decideds that it is time for
    // cleaning. Id must be id of oldest active transaction.
    void clean(const Id &id) final;

private:
    store_t list;
};
