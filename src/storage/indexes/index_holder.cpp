#include "storage/indexes/index_holder.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class TG, class K>
IndexBase<TG, K> *npr = (IndexBase<TG, K> *)nullptr;

template <class TG, class K>
bool IndexHolder<TG, K>::set_index(std::unique_ptr<IndexBase<TG, K>> inx)
{
    if (index.compare_exchange_strong(npr<TG, K>, inx.get())) {
        inx.release();
        return true;
    } else {
        return false;
    }
}

template <class TG, class K>
OptionPtr<IndexBase<TG, K>> IndexHolder<TG, K>::get_read() const
{
    auto loaded = index.load(std::memory_order_acquire);
    if (loaded == nullptr || !loaded->can_read()) {
        return OptionPtr<IndexBase<TG, K>>();
    } else {
        return make_option_ptr(loaded);
    }
}

template <class TG, class K>
OptionPtr<IndexBase<TG, K>>
IndexHolder<TG, K>::get_write(const tx::Transaction &t) const
{
    auto loaded = index.load(std::memory_order_acquire);
    if (loaded == nullptr || !loaded->is_obliged_to_insert(t)) {
        return OptionPtr<IndexBase<TG, K>>();
    } else {
        return make_option_ptr(loaded);
    }
}

template <class TG, class K>
Option<std::unique_ptr<IndexBase<TG, K>>>
IndexHolder<TG, K>::remove_index(IndexBase<TG, K> *expected)
{
    if (index.compare_exchange_strong(expected, nullptr)) {
        return make_option(std::unique_ptr<IndexBase<TG, K>>(expected));
    } else {
        return make_option(std::unique_ptr<IndexBase<TG, K>>());
    }
}

template <class TG, class K>
Option<std::unique_ptr<IndexBase<TG, K>>> IndexHolder<TG, K>::remove_index()
{
    auto removed = index.exchange(nullptr);
    if (removed == nullptr) {
        return make_option<std::unique_ptr<IndexBase<TG, K>>>();
    } else {
        return make_option(std::unique_ptr<IndexBase<TG, K>>(removed));
    }
}

template class IndexHolder<TypeGroupEdge, std::nullptr_t>;
template class IndexHolder<TypeGroupVertex, std::nullptr_t>;
