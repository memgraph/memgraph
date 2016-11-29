#include "storage/indexes/index_record.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"
#include "transactions/transaction.hpp"

template <class TG, class K>
IndexRecord<TG, K>::IndexRecord(K key, typename TG::record_t *record,
                                typename TG::vlist_t *vlist)
    : key(std::move(key)), record(record), vlist(vlist)
{
    assert(record != nullptr);
    assert(vlist != nullptr);
}

template <class TG, class K>
void IndexRecord<TG, K>::set_descending()
{
    descending = true;
}

template <class TG, class K>
bool IndexRecord<TG, K>::empty() const
{
    return record == nullptr;
}

template <class TG, class K>
bool IndexRecord<TG, K>::is_valid(tx::Transaction &t) const
{
    assert(!empty());
    // TODO: this has to be tested extensively
    // before here was only (record == newest_record)
    // but it doesn't work if single transaction updates something from
    // this index and then tries to access to the same element again through
    // the same index
    auto newest_record = vlist->find(t);
    if (newest_record == nullptr)
        return false;
    return (record == newest_record) || (newest_record->tx.cre() == t.id);
}

template <class TG, class K>
bool IndexRecord<TG, K>::to_clean(const Id &oldest_active) const
{
    assert(!empty());
    return record->is_deleted_before(oldest_active);
}

template <class TG, class K>
const auto IndexRecord<TG, K>::access(DbTransaction &db) const
{
    return typename TG::accessor_t(record, vlist, db);
}

#include "storage/edge_accessor.hpp"
#include "storage/edge_record.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertex_record.hpp"

template class IndexRecord<TypeGroupVertex, std::nullptr_t>;
template class IndexRecord<TypeGroupEdge, std::nullptr_t>;
