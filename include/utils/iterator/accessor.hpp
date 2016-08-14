#pragma once

#include "utils/iterator/wrap.hpp"
#include "utils/option.hpp"

namespace iter
{
template <class T, class I>
class OneTimeAccessor
{
public:
    OneTimeAccessor() : it(Option<Wrap<T, I>>()) {}
    OneTimeAccessor(I &&it) : it(Wrap<T, I>(std::move(it))) {}

    Wrap<T, I> begin() { return it.take(); }

    Wrap<T, I> end() { return Wrap<T, I>(); }

private:
    Option<Wrap<T, I>> it;
};

template <class I>
auto make_one_time_accessor(I &&iter)
{
    return OneTimeAccessor<decltype(iter.next().take()), I>(std::move(iter));
}
}
