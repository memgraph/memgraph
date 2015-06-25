#ifndef MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPNODE_HPP
#define MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPNODE_HPP

#include <cstdlib>
#include <atomic>

#include "utils/sync/spinlock.hpp"

// concurrent skiplist node based on the implementation described in
// "A Provably Correct Scalable Concurrent Skip List"
// https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf

template <class K, class T>
struct SkipNode
{
    enum flags {
        MARKED_FOR_REMOVAL = 1,
        FULLY_LINKED       = 1 << 1
    };

    static SkipNode* create();
    static SkipNode* destroy();

private:
    SkipNode();
    ~SkipNode();

    K* key;
    T* item;

    std::atomic<uint8_t> flags;
    const uint8_t level;
    SpinLock lock;
    
    // this creates an array of the size zero locally inside the SkipNode
    // struct. we can't put any sensible value here since we don't know
    // what size it will be untill the skipnode is allocated. we could make
    // it a SkipNode** but then we would have two memory allocations, one for
    // SkipNode and one for the forward list and malloc calls are expensive!

    // we're gonna cheat here. we'll make this a zero length list and then
    // allocate enough memory for the SkipNode struct to store more than zero
    // elements (precisely *level* elements). c++ does not check bounds so we
    // can access anything we want!
    std::atomic<SkipNode<K, T>*> forward[0];
};

template <class K, class T>
SkipNode<K, T>::SkipNode(size_t level)
{
    forward = new SkipNode*[level];
}

template <class K, class T>
SkipNode<K, T>::SkipNode(K* key, T* item, size_t level)
    : key(key), item(item)
{
    forward = new SkipNode*[level];
}

template <class K, class T>
SkipNode<K, T>::~SkipNode()
{
    delete forward;
}

#endif
