#pragma once

#include "utils/iterator/iterator_base.hpp"
#include "utils/option.hpp"

namespace iter
{

// Class which turns accessor int next() based iterator.
// T - type of return value
// I - iterator type gotten from accessor
// A - accessor type
template <class T, class I, class A>
class IteratorAccessor : public IteratorBase<T>
{
public:
    IteratorAccessor() = delete;

    IteratorAccessor(A &&acc)
        : begin(std::move(acc.begin())), acc(std::forward<A>(acc))
    {
    }

    IteratorAccessor(IteratorAccessor &&other)
        : begin(std::move(other.begin)), acc(std::forward<A>(other.acc))
    {
    }

    ~IteratorAccessor() final {}

    // Iter(const Iter &other) = delete;
    // Iter(Iter &&other) :
    // begin(std::move(other.begin)),end(std::move(other.end)) {};

    Option<T> next() final
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
    // Compiler cant deduce types T and I. decltype are here to help with it.
    return IteratorAccessor<decltype(&(*(acc.begin().operator->()))),
                            decltype(acc.begin()), A>(std::move(acc));
}

template <class A>
auto make_iter_ref(A &acc)
{
    // Compiler cant deduce types T and I. decltype are here to help with it.
    return IteratorAccessor<decltype(&(*(acc.begin().operator->()))),
                            decltype(acc.begin()), A &>(acc);
}
}
