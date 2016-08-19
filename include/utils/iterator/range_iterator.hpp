#pragma once

#include "utils/option.hpp"

namespace iter
{

// Class which wraps iterator with next() into c++ iterator.
// T - type of return value
// I - iterator type
template <class T, class I>
class RangeIterator
{

public:
    RangeIterator() : iter(Option<I>()), value(Option<T>()){};

    RangeIterator(I &&iter)
        : value(iter.next()), iter(Option<I>(std::move(iter)))
    {
    }

    T &operator*()
    {
        assert(value.is_present());
        return value.get();
    }

    T *operator->()
    {
        assert(value.is_present());
        return &value.get();
    }

    operator T &()
    {
        assert(value.is_present());
        return value.get();
    }

    RangeIterator &operator++()
    {
        assert(iter.is_present());
        value = iter.get().next();
        return (*this);
    }

    RangeIterator &operator++(int) { return operator++(); }

    friend bool operator==(const RangeIterator &a, const RangeIterator &b)
    {
        return a.value.is_present() == b.value.is_present();
    }

    friend bool operator!=(const RangeIterator &a, const RangeIterator &b)
    {
        return !(a == b);
    }

private:
    Option<I> iter;
    Option<T> value;
};

template <class I>
auto make_range_iterator(I &&iter)
{
    // Because function isn't receving or in any way using type T from
    // RangeIterator compiler can't deduce it thats way there is decltype in
    // construction of RangeIterator. Resoulting type of iter.next().take() is
    // T.
    return RangeIterator<decltype(iter.next().take()), I>(std::move(iter));
}
}
