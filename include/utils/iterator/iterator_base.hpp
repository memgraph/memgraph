#pragma once

#include "utils/iterator/count.hpp"
#include "utils/option.hpp"

class EdgeType;

namespace iter
{
template <class I, class OP>
auto make_map(I &&iter, OP &&op);

template <class I, class OP>
auto make_filter(I &&iter, OP &&op);

template <class I, class C>
void for_all(I &&iter, C &&consumer);
}

// Base iterator for next() kind iterator.
// T - type of return value
template <class T>
class IteratorBase
{
public:
    virtual ~IteratorBase(){};

    virtual Option<T> next() = 0;

    virtual Count count() { return Count(0, ~((size_t)0)); }

    template <class OP>
    auto map(OP &&op)
    {
        return iter::make_map<decltype(std::move(*this)), OP>(std::move(*this),
                                                              std::move(op));
    }

    template <class OP>
    auto filter(OP &&op)
    {
        return iter::make_filter<decltype(std::move(*this)), OP>(
            std::move(*this), std::move(op));
    }

    // Maps with call to method to() and filters with call to fill.
    auto to()
    {
        return map([](auto er) { return er.to(); }).fill();
    }

    // Filters with call to method fill()
    auto fill()
    {
        return filter([](auto &ra) { return ra.fill(); });
    }

    // Filters with type
    template <class TYPE>
    auto type(TYPE const &type)
    {
        return filter([&](auto &ra) { return ra.edge_type() == type; });
    }

    // Filters out vertices which are connected.
    auto isolated()
    {
        return filter([&](auto &ra) { return ra.isolated(); });
    }

    // For all items calls OP.
    template <class OP>
    void for_all(OP &&op)
    {
        iter::for_all(std::move(*this), std::move(op));
    }
};
