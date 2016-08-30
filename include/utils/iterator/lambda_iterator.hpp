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
    LambdaIterator(F &&f, size_t count) : func(std::move(f)), _count(count) {}

    LambdaIterator(LambdaIterator &&other)
        : func(std::move(other.func)), _count(other._count)
    {
    }

    ~LambdaIterator() final {}

    Option<T> next() final
    {
        _count = _count > 0 ? _count - 1 : 0;
        return func();
    }

    Count count() final { return Count(_count); }

private:
    F func;
    size_t _count;
};

// Wraps lambda which returns options as an iterator.
template <class F>
auto make_iterator(F &&f, size_t count)
{
    // Because function isn't receving or in any way using type T from
    // FunctionIterator compiler can't deduce it thats way there is decltype in
    // construction of FunctionIterator. Resoulting type of iter.next().take()
    // is T.
    return LambdaIterator<decltype(f().take()), F>(std::move(f), count);
}
}
