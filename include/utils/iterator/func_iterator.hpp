#pragma once

#include "utils/iterator/iterator_base.hpp"

namespace iter
{
// Wraps function into interator with next().
// T - type of return value
// F - type of wraped function
template <class T, class F>
class FunctionIterator : public IteratorBase<T>
{
public:
    FunctionIterator(F &&f) : func(std::move(f)) {}

    Option<T> next() final { return func(); }

private:
    F func;
};

// Wraps function which returns options as an iterator.
template <class F>
auto make_iterator(F &&f)
{
    // Because function isn't receving or in any way using type T from
    // FunctionIterator compiler can't deduce it thats way there is decltype in
    // construction of FunctionIterator. Resoulting type of iter.next().take()
    // is T.
    return FunctionIterator<decltype(f().take()), F>(std::move(f));
}
}
