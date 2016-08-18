#pragma once

#include "utils/option.hpp"

// Base iterator for next() kind iterator.
// T - type of return value
template <class T>
class IteratorBase
{
public:
    virtual Option<T> next() = 0;
};
