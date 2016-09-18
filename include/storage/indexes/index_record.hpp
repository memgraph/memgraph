#pragma once

#include "mvcc/id.hpp"
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

    // true if record is nullptr.
    bool empty() const;

    // True if this index record i valid for given Transaction.
    bool is_valid(tx::Transaction &t) const;

    // True if it can be removed.
    bool to_clean(const Id &oldest_active) const;

    // This method is valid only if is_valid is true.
    const auto access(DbTransaction &db) const;

    const K key;

private:
    bool descending = false; // TODO: this can be passed as template argument.
    typename TG::record_t *const record{nullptr};
    typename TG::vlist_t *const vlist{nullptr};
};
