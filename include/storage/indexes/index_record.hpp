#pragma once

#include "utils/border.hpp"
#include "utils/total_ordering.hpp"

namespace tx
{
class Transaction;
}
class DbTransaction;

// TG type group
// K key on which record is ordered.
template <class TG, class K>
class IndexRecord : public TotalOrdering<IndexRecord<TG, K>>,
                    public TotalOrdering<Border<K>, IndexRecord<TG, K>>
{
public:
    IndexRecord() = default;

    IndexRecord(K key, typename TG::record_t *record,
                typename TG::vlist_t *vlist);

    friend bool operator<(const IndexRecord &lhs, const IndexRecord &rhs)
    {
        return (lhs.key < rhs.key ||
                (lhs.key == rhs.key && lhs.vlist == rhs.vlist &&
                 lhs.record < rhs.record)) ^
               lhs.descending;
    }

    friend bool operator==(const IndexRecord &lhs, const IndexRecord &rhs)
    {
        return lhs.key == rhs.key &&
               (lhs.vlist != rhs.vlist || lhs.record == rhs.record);
    }

    friend bool operator<(const Border<K> &lhs, const IndexRecord &rhs)
    {
        return lhs < rhs.key;
    }

    friend bool operator==(const Border<K> &lhs, const IndexRecord &rhs)
    {
        return lhs == rhs.key;
    }

    // Will change ordering of record to descending.
    void set_descending();

    bool empty() const;

    bool is_valid(tx::Transaction &t) const;

    const auto access(DbTransaction &db) const;

    const K key;

private:
    bool descending = false;
    typename TG::record_t *const record{nullptr};
    typename TG::vlist_t *const vlist{nullptr};
};
