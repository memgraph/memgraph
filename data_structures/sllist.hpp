#ifndef MEMGRAPH_DATA_STRUCTURES_SLLIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_SLLIST_HPP

#include <list>

#include "sync/spinlock.hpp"
#include "sync/lockable.hpp"

template <class T>
class SpinLockedList : Lockable<SpinLock>
{
public:

    void push_front(T item)
    {
        auto guard = this->acquire();
        head = new Node(item, head);
    }
    
    bool remove(const T&)
    {
        // HM!
    }

private:
    struct Node : Lockable<SpinLock>
    {
        Node(T item, Node* next) : item(item), next(next) {}

        T item;
        Node* next;
    };

    Node* head;

    // TODO add removed items to a list for garbage collection
    // std::list<Node*> removed;
};

#endif
