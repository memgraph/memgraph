#pragma once

#include "utils/iterator/filter.hpp"
#include "storage/iterator/graph_composable.hpp"

template <class T, class I, class OP>
class GraphFilter : public Filter<T, I, OP>,
                    public GraphComposable<T, GraphFilter<T, I, OP>
{
    using Filter::Filter;
};

template <class I, class OP>
auto make_graph_filter(I &&iter, OP &&op)
{
    // Compiler cant deduce type T. decltype is here to help with it.
    return GraphFilter<decltype(iter.next().take()), I, OP>(std::move(iter),
                                                            std::move(op));
}
