#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter
{

// Class which filters values T returned by I iterator if OP returns true for
// them.
// T - type of return value
// I - iterator type
// OP - type of filter function. OP: T& -> bool
template <class T, class I, class OP>
class Filter : public IteratorBase<T>, public Composable<T, Filter<T, I, OP>>
{

public:
    Filter() = delete;

    // Filter operation is designed to be used in chained calls which operate on
    // a
    // iterator. Filter will in that usecase receive other iterator by value and
    // std::move is a optimization for it.
    Filter(I &&iter, OP &&op) : iter(std::move(iter)), op(std::move(op)) {}

    // Return values for which filter return true.
    Option<T> next() final
    {
        auto item = Option<T>();
        do {
            item = iter.next();
            if (!item.is_present()) {
                return Option<T>();
            }
        } while (!op(item.get()));

        return std::move(item);
    }

    Count count() final { return iter.count().min_zero(); }

private:
    I iter;
    OP op;
};

template <class I, class OP>
auto make_filter(I &&iter, OP &&op)
{
    // Compiler cant deduce type T. decltype is here to help with it.
    return Filter<decltype(iter.next().take()), I, OP>(std::move(iter),
                                                       std::move(op));
}
}
