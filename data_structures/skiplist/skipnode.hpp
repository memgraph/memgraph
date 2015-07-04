#ifndef MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPNODE_HPP
#define MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPNODE_HPP

#include <cstdlib>
#include <atomic>
#include <mutex>

#include "sync/spinlock.hpp"

// concurrent skiplist node based on the implementation described in
// "A Provably Correct Scalable Concurrent Skip List"
// https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf

template <class K,
          class T,
          class lock_type=SpinLock>
struct SkipNode
{
    using Node = SkipNode<K, T, lock_type>;

    enum flags {
        MARKED       = 1,
        FULLY_LINKED = 1 << 1
    };

    // key against the value is sorted in the skiplist. must be comparable
    K* key;

    // item on the heap this node points
    T* item;

    const uint8_t height;

    // use this for creating new nodes. DON'T use the constructor (it's
    // private anyway)
    static SkipNode* create(int height, K* key, T* item)
    {
        size_t size = sizeof(Node) + height * sizeof(std::atomic<Node*>);
        
        auto* node = static_cast<SkipNode*>(malloc(size));
        new (node) Node(height, key, item);
        
        return node;
    }

    // acquire an exclusive guard on this node, use for concurrent access
    std::unique_lock<lock_type> guard()
    {
        return std::unique_lock<lock_type>(lock);
    }

    // use this for destroying nodes after you don't need them any more
    static void destroy(Node* node)
    {
        node->~SkipNode();
        free(node);
    }

    bool marked() const
    {
        return fget() & MARKED;
    }

    void set_marked()
    {
        fset(fget() | MARKED);
    }
    
    bool fully_linked() const
    {
        return fget() & FULLY_LINKED;
    }

    void set_fully_linked()
    {
        fset(fget() | FULLY_LINKED);
    }

    Node* forward(uint8_t level)
    {
        return forward_[level].load(std::memory_order_consume);
    }

    void forward(uint8_t level, Node* next)
    {
        forward_[level].store(next, std::memory_order_release);
    }

private:
    SkipNode(uint8_t height, K* key, T* item)
        : key(key), item(item), height(height)
    {
        // set the flags to zero at the beginning
        fset(0);

        // we need to explicitly call the placement new operator over memory
        // allocated for forward_ pointers, see the notes below
        for (uint8_t i = 0; i < height; ++i)
            new (&forward_[i]) std::atomic<Node*>(nullptr);
    }

    ~SkipNode()
    {
        for (uint8_t i = 0; i < height; ++i)
            forward_[i].~atomic();
    }

    uint8_t fget() const
    {
        // do an atomic load of the flags. if you need to use this value
        // more than one time in a function it's a good idea to store it
        // in a stack variable (non atomic) to optimize for performance
        return flags.load(std::memory_order_consume);
    }

    void fset(uint8_t value)
    {
        // atomically set new flags
        flags.store(value, std::memory_order_release);
    }

    std::atomic<uint8_t> flags;
    lock_type lock;
    
    // this creates an array of the size zero locally inside the SkipNode
    // struct. we can't put any sensible value here since we don't know
    // what size it will be untill the skipnode is allocated. we could make
    // it a SkipNode** but then we would have two memory allocations, one for
    // SkipNode and one for the forward list and malloc calls are expensive!

    // we're gonna cheat here. we'll make this a zero length list and then
    // allocate enough memory for the SkipNode struct to store more than zero
    // elements (precisely *height* elements). c++ does not check bounds so we
    // can access anything we want!
    std::atomic<Node*> forward_[0];
};

#endif
