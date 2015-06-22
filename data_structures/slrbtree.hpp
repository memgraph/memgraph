#ifndef MEMGRAPH_DATA_STRUCTURES_SLRBTREE_HPP
#define MEMGRAPH_DATA_STRUCTURES_SLRBTREE_HPP

#include <map>

#include "utils/sync/spinlock.hpp"

template <class K, class T>
class SlRbTree
{
public:

private:
    SpinLock lock;
    std::map<K, T> tree;
};

#endif
