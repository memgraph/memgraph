#ifndef MEMGRAPH_DATA_STRUCTURES_SKIPLIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_SKIPLIST_HPP

#include <algorithm>
#include <cstdlib>
#include <array>

#include "utils/random/xorshift.hpp"

size_t new_height(int max_height)
{
    uint64_t rand = xorshift::next();
    size_t height = 0;

    while(max_height-- && (rand >>= 1) & 1)
        height++;

    return height;
}


template <class K, class T>
struct SkipNode
{
    SkipNode(K* key, T* item)
        : key(key), item(item) {}

    K* key;
    T* item;

    SkipNode* up;
    SkipNode* forward;
};

template <class K,
          class T,
          size_t height = 16>
class SkipList
{
    using Node = SkipNode<K, T>;
    using Tower = std::array<Node, height>;

public:
    SkipList()
    {
        head.fill(nullptr);
    }

    T* get(const K* const key)
    {
        size_t h = height;

        while(h--)
        {

        }
    }

    void put(const K* key, T* item)
    {
        auto* node = new SkipNode<T, K>(key, item);

        Tower trace;

        size_t h = height - 1;

        while(h--)
        {
            
        }
    }

    void del(const K* const key)
    {

    }

private:
    Tower head;
};

#endif
