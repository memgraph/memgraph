#include "storage/indexes/impl/nonunique_unordered_index.hpp"

#include "database/db_accessor.hpp"
#include "database/db_transaction.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/edge_record.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertex_record.hpp"
#include "utils/iterator/iterator.hpp"

#include "storage/indexes/index_record.cpp"

template <class T, class K>
NonUniqueUnorderedIndex<T, K>::NonUniqueUnorderedIndex()
    : IndexBase<T, K>(false, None)
{
}

template <class T, class K>
NonUniqueUnorderedIndex<T, K>::NonUniqueUnorderedIndex(tx::Transaction const &t)
    : IndexBase<T, K>(false, None, t)
{
}

template <class T, class K>
bool NonUniqueUnorderedIndex<T, K>::insert(IndexRecord<T, K> &&value)
{
    list.begin().push(std::move(value));
    return true;
}

template <class T, class K>
iter::Virtual<const typename T::accessor_t>
NonUniqueUnorderedIndex<T, K>::for_range(DbAccessor &t, Border<K> from,
                                         Border<K> to)
{
    return iter::make_virtual(
        for_range_exact(t, std::move(from), std::move(to)));
}

template <class T, class K>
auto NonUniqueUnorderedIndex<T, K>::for_range_exact(DbAccessor &t_v,
                                                    Border<K> from_v,
                                                    Border<K> to_v)
{
    return iter::make_iterator(
        [
          it = list.cbegin(), end = list.cend(), from = from_v, to = to_v,
          t = t_v
        ]() mutable->auto {
            while (it != end) {
                const IndexRecord<T, K> &r = *it;
                if (from < r.key && to > r.key &&
                    r.is_valid(t.db_transaction.trans)) {
                    const typename T::accessor_t acc =
                        r.access(t.db_transaction);
                    it++;
                    return make_option(std::move(acc));
                }
                it++;
            }

            return Option<const typename T::accessor_t>();
        },
        list.size());
}

template <class T, class K>
void NonUniqueUnorderedIndex<T, K>::clean(const Id &id)
{
    auto end = list.end();
    for (auto it = list.begin(); it != end; it++) {
        if (it->to_clean(id)) {
            it.remove();
        }
    }
}

template class NonUniqueUnorderedIndex<TypeGroupEdge, std::nullptr_t>;
template class NonUniqueUnorderedIndex<TypeGroupVertex, std::nullptr_t>;
