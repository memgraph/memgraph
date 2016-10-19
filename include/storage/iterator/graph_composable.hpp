#pragma once

#include "utils/iterator/composable.hpp"

// Class for creating easy composable iterators for querying.
//
// This class contains graph specific methods.
//
// Derived - type of derived class
// T - return type
template <class T, Derived>
class GraphComposable : public Composable<T, GraphComposable<T, Derived>>
{
    // TODO: various filters
    // from filter
    // to filter
    // match filter
};


