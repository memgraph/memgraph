#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "utils/total_ordering.hpp"

// class DbTransaction;
// namespace tx
// {
// class Transaction;
// }

// T type of record.
// K key on which record is ordered.
template <class T, class K>
class IndexRecord : public TotalOrdering<IndexRecord<T, K>>
{
public:
    using vlist_t = mvcc::VersionList<T>;

    IndexRecord() = default;

    IndexRecord(K key, T *record, vlist_t *vlist)
        : key(std::move(key)), record(record), vlist(vlist)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
    }

    friend bool operator<(const IndexRecord &lhs, const IndexRecord &rhs)
    {
        return lhs.key < rhs.key ||
               (lhs.key == rhs.key && lhs.vlist == rhs.vlist &&
                lhs.record < rhs.record);
    }

    friend bool operator==(const IndexRecord &lhs, const IndexRecord &rhs)
    {
        return lhs.key == rhs.key &&
               (lhs.vlist != rhs.vlist || lhs.record == rhs.record);
    }

    bool empty() const { return record == nullptr; }

    bool is_valid(tx::Transaction &t) const
    {
        assert(!empty());
        return record == vlist->find(t);
    }

    const auto access(DbTransaction &db) const
    {
        return T::Accessor::create(record, vlist, db);
    }

    const K key;

private:
    T *const record{nullptr};
    vlist_t *const vlist{nullptr};
};

template <class K>
using VertexIndexRecord = IndexRecord<Vertex, K>;

template <class K>
using EdgeIndexRecord = IndexRecord<Edge, K>;
