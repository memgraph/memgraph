#pragma once

#include "storage/vertex_accessor.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/option.hpp"

namespace query_help
{

template <class A>
bool fill(A &a)
{
    return a.fill();
}
};

// Base iterator for next() kind iterator.
// Vertex::Accessor - type of return value
template <>
class IteratorBase<Vertex::Accessor>
{
public:
    virtual Option<Vertex::Accessor> next() = 0;

    auto fill()
    {
        return iter::make_filter(std::move(*this), query_help::fill);
    }
};
