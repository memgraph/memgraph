#pragma once

#include "utils/iterator/count.hpp"
#include "utils/option.hpp"

class EdgeType;

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
    auto map(OP &&op);

    template <class OP>
    auto filter(OP &&op);

    // Maps with call to method to() and filters with call to fill.
    auto to();

    // Filters with call to method fill()
    auto fill();

    // Filters with type
    template <class TYPE>
    auto type(TYPE const &type);

    // For all items calls OP.
    template <class OP>
    void for_all(OP &&op);
};
