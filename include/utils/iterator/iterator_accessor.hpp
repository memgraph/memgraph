#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter
{

// Class which turns accessor int next() based iterator.
// T - type of return value
// I - iterator type gotten from accessor
// A - accessor type
template <class T, class I, class A>
class IteratorAccessor : public IteratorBase<T>,
                         public Composable<T, IteratorAccessor<T, I, A>>
{
public:
    IteratorAccessor() = delete;

    IteratorAccessor(A &&acc)
        : begin(std::move(acc.begin())), acc(std::forward<A>(acc)), returned(0)
    {
    }

    IteratorAccessor(IteratorAccessor &&other)
        : begin(std::move(other.begin)), acc(std::forward<A>(other.acc)),
          returned(other.returned)
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
            returned++;
            return ret;
        } else {
            return Option<T>();
        }
    }

    Count count() final
    {
        auto size = acc.size();
        if (size > returned) {
            return Count(0);
        } else {
            return Count(size - returned);
        }
    }

private:
    I begin;
    A acc;
    size_t returned;
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
