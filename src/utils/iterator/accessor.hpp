#pragma once

#include "utils/iterator/range_iterator.hpp"
#include "utils/option.hpp"

namespace iter
{
// Class which turns ranged iterator with next() into accessor.
// T - type of return value
// I - iterator type
template <class T, class I>
class OneTimeAccessor
{
public:
    OneTimeAccessor() : it(Option<RangeIterator<T, I>>()) {}
    OneTimeAccessor(I &&it) : it(RangeIterator<T, I>(std::move(it))) {}

    RangeIterator<T, I> begin() { return it.take(); }

    RangeIterator<T, I> end() { return RangeIterator<T, I>(); }

private:
    Option<RangeIterator<T, I>> it;
};

template <class I>
auto make_one_time_accessor(I &&iter)
{
    // Because function isn't receving or in any way using type T from
    // OneTimeAccessor compiler can't deduce it thats way there is decltype in
    // construction of OneTimeAccessor. Resoulting type of iter.next().take() is
    // T.
    return OneTimeAccessor<decltype(iter.next().take()), I>(std::move(iter));
}
}
