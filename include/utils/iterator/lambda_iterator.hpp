#pragma once

#include "utils/iterator/iterator_base.hpp"

namespace iter
{
// Wraps lambda into interator with next().
// T - type of return value
// F - type of wraped lambda
template <class T, class F>
class LambdaIterator : public IteratorBase<T>
{
public:
    LambdaIterator(F &&f) : func(std::move(f)) {}

    LambdaIterator(LambdaIterator &&other) : func(std::move(other.func)) {}

    ~LambdaIterator() final {}

    Option<T> next() final { return func(); }

private:
    F func;
};

// Wraps lambda which returns options as an iterator.
template <class F>
auto make_iterator(F &&f)
{
    // Because function isn't receving or in any way using type T from
    // FunctionIterator compiler can't deduce it thats way there is decltype in
    // construction of FunctionIterator. Resoulting type of iter.next().take()
    // is T.
    return LambdaIterator<decltype(f().take()), F>(std::move(f));
}
}
