#include "storage/indexes/index_record.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

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
    return record == vlist->find(t);
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
