#pragma once

#include <vector>

#include "build.hpp"
#include "nns.hpp"

namespace kd
{

template <class T, class U>
class KdTree
{
public:
    KdTree() {}

    template <class It>
    KdTree(It first, It last);

    const U& lookup(const Point<T>& pk) const;

protected:
    std::unique_ptr<KdNode<float, U>> root;
};

template <class T, class U>
const U& KdTree<T, U>::lookup(const Point<T>& pk) const
{
    // do a nearest neighbour search on the tree
    return kd::nns(pk, root.get())->data;
}

template <class T, class U>
template <class It>
KdTree<T, U>::KdTree(It first, It last)
{
    root.reset(kd::build<T, U, It>(first, last));
}

}
