#ifndef MEMGRAPH_DATA_STRUCTURES_LOCKFREE_LIST2_HPP
#define MEMGRAPH_DATA_STRUCTURES_LOCKFREE_LIST2_HPP

#include <atomic>

#include "threading/sync/spinlock.hpp"
#include "threading/sync/lockable.hpp"

namespace lockfree
{

template <class T>
class List2 : Lockable<SpinLock>
{
    struct Node { T value; std::atomic<Node*> next; };

    class iterator
    {
        iterator(Node* node) : node(node) {}

        T& operator*() const { return *node; }
        T* operator->() const { return node; }

        bool end() const
        {
            return node == nullptr;
        }

        iterator& operator++()
        {
            node = node->next.load(std::memory_order_relaxed);
            return *this;
        }

        iterator& operator++(int)
        {
            return operator++();
        }

    protected:
        Node* node;
    };

public:

    ~List2()
    {
        Node* next, node = head.load(std::memory_order_relaxed);

        for(; node != nullptr; node = next)
        {
            next = node->next;
            delete node;
        }
    }

    void insert(T value)
    {
        auto guard = acquire();
        head.store(new Node { value, head });
    }

    iterator begin() { return iterator(head.load()); }

private:
    std::atomic<Node*> head;
};

}

#endif
