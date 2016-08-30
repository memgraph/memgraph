#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter
{

// Class which maps values returned by I iterator into value of type T with OP
// function and ends when op return empty optional.
// T - type of return value
// I - iterator type
// OP - type of mapper function. OP: V -> Option<T>
template <class T, class I, class OP>
class LimitedMap : public IteratorBase<T>,
                   public Composable<T, LimitedMap<T, I, OP>>
{

public:
    LimitedMap() = delete;

    // LimitedMap operation is designed to be used in chained calls which
    // operate on a
    // iterator. LimitedMap will in that usecase receive other iterator by value
    // and
    // std::move is a optimization for it.
    LimitedMap(I &&iter, OP &&op) : iter(std::move(iter)), op(std::move(op)) {}

    LimitedMap(LimitedMap &&m) : iter(std::move(m.iter)), op(std::move(m.op)) {}

    ~LimitedMap() final {}

    Option<T> next() final
    {
        auto item = iter.next();
        if (item.is_present()) {
            return op(item.take());
        } else {
            return Option<T>();
        }
    }

    Count count() final { return iter.count(); }

private:
    I iter;
    OP op;
};

template <class I, class OP>
auto make_limited_map(I &&iter, OP &&op)
{
    // Compiler cant deduce type T. decltype is here to help with it.
    return LimitedMap<decltype(op(iter.next().take()).take()), I, OP>(
        std::move(iter), std::move(op));
}
}
