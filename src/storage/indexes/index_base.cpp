#include "storage/indexes/index_base.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"
#include "transactions/transaction.hpp"

template <class TG, class K>
IndexBase<TG, K>::IndexBase(bool unique, Order order)
    : _unique(unique), _order(order), created(Id(0)), active(true)
{
}

template <class TG, class K>
IndexBase<TG, K>::IndexBase(bool unique, Order order, const tx::Transaction &t)
    : _unique(unique), _order(order), created(t.id)
{
}

template <class TG, class K>
void IndexBase<TG, K>::activate()
{
    assert(!can_read());
    active.store(true);
}

template <class TG, class K>
bool IndexBase<TG, K>::can_read()
{
    return active.load(std::memory_order_acquire);
}

template <class TG, class K>
bool IndexBase<TG, K>::is_obliged_to_insert(const tx::Transaction &t)
{
    return t.id >= created;
}

template class IndexBase<TypeGroupEdge, std::nullptr_t>;
template class IndexBase<TypeGroupVertex, std::nullptr_t>;
