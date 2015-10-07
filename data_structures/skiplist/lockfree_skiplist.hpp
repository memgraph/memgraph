#ifndef MEMGRAPH_DATA_STRUCTURES_SKIPLIST_LOCKFREE_SKIPLIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_SKIPLIST_LOCKFREE_SKIPLIST_HPP

#include <cstdlib>
#include <atomic>

#include "utils/mark_ref.hpp"

namespace lockfree
{

template <class K, class T>
class SkipList
{
public:
    struct Node
    {
        using ref_t = MarkRef<Node>;

        K* key;
        T* item;

        const uint8_t height;

        static Node* create(uint8_t height, K* key, T* item)
        {
            auto size = sizeof(Node) + height * sizeof(std::atomic<ref_t>);
            auto node = static_cast<Node*>(std::malloc(size));
            return new (node) Node(height, key, item);
        }

        static void destroy(Node* node)
        {
            node->~SkipNode();
            free(node);
        }

    private:
        Node(uint8_t height, K* key, T* item)
            : key(key), item(item), height(height)
        {
            for(uint8_t i = 0; i < height; ++i)
                new (&tower[i]) std::atomic<ref_t>(nullptr);
        }

        // this creates an array of the size zero. we can't put any sensible
        // value here since we don't know what size it will be untill the
        // node is allocated. we could make it a SkipNode** but then we would
        // have two memory allocations, one for node and one for the forward
        // list. this way we avoid expensive malloc/free calls and also cache
        // thrashing when following a pointer on the heap
        std::atomic<ref_t> tower[0];
    };

    //void list_search(const K& key, 

};

}

#endif
