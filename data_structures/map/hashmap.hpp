#ifndef MEMGRAPH_DATA_STRUCTURES_LOCKFREE_HASHMAP_HPP
#define MEMGRAPH_DATA_STRUCTURES_LOCKFREE_HASHMAP_HPP

#include <unordered_map>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

namespace lockfree
{

template <class K, class V>
class HashMap: Lockable<SpinLock> 
{
public:

    V at(const K& key)
    {
        auto guard = acquire();

        return hashmap[key];
    }

    void put(const K& key, const K& value)
    {
        auto quard = acquire();

        hashmap[key] = value;
    }

private:
    std::unordered_map<K, V> hashmap;
};

}

#endif
