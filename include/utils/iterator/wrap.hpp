#pragma once

#include "utils/option.hpp"

namespace iter
{
template <class T, class I>
class Wrap
{

public:
    Wrap() : iter(Option<I>()), value(Option<T>()){};

    Wrap(I &&iter) : value(iter.next()), iter(Option<I>(std::move(iter))) {}

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

    Wrap &operator++()
    {
        assert(iter.is_present());
        value = iter.get().next();
        return (*this);
    }

    Wrap &operator++(int) { return operator++(); }

    friend bool operator==(const Wrap &a, const Wrap &b)
    {
        return a.value.is_present() == b.value.is_present();
    }

    friend bool operator!=(const Wrap &a, const Wrap &b) { return !(a == b); }

private:
    Option<I> iter;
    Option<T> value;
};

template <class I>
auto make_wrap(I &&iter)
{
    return Wrap<decltype(iter.next().take()), I>(std::move(iter));
}
}
