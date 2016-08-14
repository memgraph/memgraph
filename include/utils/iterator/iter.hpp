#pragma once

#include "utils/option.hpp"

namespace iter
{
template <class T, class I, class A>
class Iter
{
public:
    Iter() = delete;

    Iter(A &&acc) : begin(std::move(acc.begin())), acc(std::forward<A>(acc)) {}
    // Iter(const Iter &other) = delete;
    // Iter(Iter &&other) :
    // begin(std::move(other.begin)),end(std::move(other.end)) {};

    auto next()
    {
        if (begin != acc.end()) {
            auto ret = Option<T>(&(*(begin.operator->())));
            begin++;
            return ret;
        } else {
            return Option<T>();
        }
    }

private:
    I begin;
    A acc;
};

// TODO: Join to make functions into one
template <class A>
auto make_iter(A &&acc)
{
    return Iter<decltype(&(*(acc.begin().operator->()))), decltype(acc.begin()),
                A>(std::move(acc));
}

template <class A>
auto make_iter_ref(A &acc)
{
    return Iter<decltype(&(*(acc.begin().operator->()))), decltype(acc.begin()),
                A &>(acc);
}
}
