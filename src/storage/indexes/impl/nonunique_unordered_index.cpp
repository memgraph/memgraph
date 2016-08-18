#include "storage/indexes/impl/nonunique_unordered_index.hpp"

#include "database/db_accessor.hpp"
#include "database/db_transaction.hpp"
#include "utils/iterator/iterator.hpp"

template <class T, class K>
NonUniqueUnorderedIndex<T, K>::NonUniqueUnorderedIndex()
    : IndexBase<T, K>(false, None)
{
}

template <class T, class K>
bool NonUniqueUnorderedIndex<T, K>::insert(IndexRecord<T, K> &&value)
{
    list.begin().push(std::move(value));
    return true;
}

template <class T, class K>
std::unique_ptr<IteratorBase<const typename T::Accessor>>
NonUniqueUnorderedIndex<T, K>::for_range(DbAccessor &t, Border<K> from,
                                         Border<K> to)
{
    return std::make_unique<decltype(
        for_range_exact(t, std::move(from), std::move(to)))>(
        for_range_exact(t, std::move(from), std::move(to)));
}

template <class T, class K>
auto NonUniqueUnorderedIndex<T, K>::for_range_exact(DbAccessor &t_v,
                                                    Border<K> from_v,
                                                    Border<K> to_v)
{
    return iter::make_iterator([
        it = list.cbegin(), end = list.cend(), from = from_v, to = to_v, t = t_v
    ]() mutable->auto {
        while (it != end) {
            const IndexRecord<T, K> &r = *it;
            if (from < r.key && to > r.key &&
                r.is_valid(t.db_transaction.trans)) {
                const typename T::Accessor acc = r.access(t.db_transaction);
                it++;
                return make_option(std::move(acc));
            }
            it++;
        }

        return Option<const typename T::Accessor>();
    });
}

template <class T, class K>
void NonUniqueUnorderedIndex<T, K>::clean(DbTransaction &)
{
    // TODO: Actual cleaning
}

#include "storage/vertex.hpp"
// #include "utils/singleton.hpp"
template class NonUniqueUnorderedIndex<Vertex, std::nullptr_t>;
