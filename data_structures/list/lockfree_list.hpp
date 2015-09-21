#ifndef MEMGRAPH_DATA_STRUCTURES_LIST_LOCKFREE_LIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_LIST_LOCKFREE_LIST_HPP

#include <atomic>
#include <unistd.h>

#include "threading/sync/lockable.hpp"

namespace lockfree
{

template <class T, size_t sleep_time = 250>
class List : Lockable<SpinLock>
{
public:
    List() = default;

    List(List&) = delete;
    List(List&&) = delete;

    void operator=(List&) = delete;

    class read_iterator
    {
    public:
        read_iterator(T* curr) : curr(curr) {}
  
        T& operator*() { return *curr; }
        T* operator->() { return curr; }

        operator T*() { return curr; }

        read_iterator& operator++()
        {
            // todo add curr->next to the hazard pointer list
            // (synchronization with GC)

            curr = curr->next.load();
            return *this;
        }
  
        read_iterator& operator++(int)
        {
            return operator++();
        }
  
    private:
        T* curr;
    };
  
    class read_write_iterator
    {
        friend class List<T, sleep_time>;

    public:
        read_write_iterator(T* prev, T* curr) : prev(prev), curr(curr) {}
  
        T& operator*() { return *curr; }
        T* operator->() { return curr; }

        operator T*() { return curr; }

        read_write_iterator& operator++()
        {
            // todo add curr->next to the hazard pointer list
            // (synchronization with GC)

            prev = curr;
            curr = curr->next.load();
            return *this;
        }
  
        read_write_iterator& operator++(int)
        {
            return operator++();
        }
  
    private:
        T* prev, curr;
    };

    read_iterator begin()
    {
        return read_iterator(head.load());
    }

    read_write_iterator rw_begin()
    {
        return read_write_iterator(nullptr, head.load());
    }

    void push_front(T* node)
    {
        // we want to push an item to front of a list like this
        // HEAD --> [1] --> [2] --> [3] --> ...
        
        // read the value of head atomically and set the node's next pointer
        // to point to the same location as head

        // HEAD --------> [1] --> [2] --> [3] --> ...
        //                 |
        //                 |
        //      NODE ------+

        T* h = node->next = head.load();

        // atomically do: if the value of node->next is equal to current value
        // of head, make the head to point to the node.
        // if this fails (another thread has just made progress), update the
        // value of node->next to the current value of head and retry again
        // until you succeed

        // HEAD ----|CAS|----------> [1] --> [2] --> [3] --> ...
        //  |         |               |
        //  |         v               |
        //  +-------|CAS|---> NODE ---+

        while(!head.compare_exchange_weak(h, node))
        {
            node->next.store(h);
            usleep(sleep_time);
        }

        // the final state of the list after compare-and-swap looks like this

        // HEAD          [1] --> [2] --> [3] --> ...
        //  |             |
        //  |             |
        //  +---> NODE ---+
    }

    bool remove(read_write_iterator& it)
    {
        // acquire an exclusive guard.
        // we only care about push_front and iterator performance so we can
        // we only care about push_front and iterator performance so we can
        // tradeoff some remove speed for better reads and inserts. remove is
        // used exclusively by the GC thread(s) so it can be slower
        auto guard = acquire();

        // even though concurrent removes are synchronized, we need to worry
        // about concurrent reads (solved by using atomics) and concurrent 
        // inserts to head (VERY dangerous, suffers from ABA problem, solved
        // by simply not deleting the head node until it gets pushed further
        // down the list)

        // check if we're deleting the head node. we can't do that because of
        // the ABA problem so just return false for now. the logic behind this
        // is that this node will move further down the list next time the
        // garbage collector traverses this list and therefore it will become
        // deletable
        if(it->prev == nullptr)
            return false;

        //  HEAD --> ... --> [i] --> [i + 1] --> [i + 2] --> ...
        //                  
        //                  prev       curr        next

        auto prev = it->prev;
        auto curr = it->curr;
        auto next = curr->next.load(std::memory_order_acquire);

        // effectively remove the curr node from the list

        //                    +---------------------+
        //                    |                     |
        //                    |                     v
        //  HEAD --> ... --> [i]     [i + 1] --> [i + 2] --> ...
        //                  
        //                  prev       curr        next

        prev->next.store(next, std::memory_order_release);

        // TODO curr is now removed from the list so no iterators will be able
        // to reach it at this point, but we still need to check the hazard
        // pointers and wait until everyone who currently holds a reference to
        // it has stopped using it before we can physically delete it
        
        // while(hp.find(reinterpret_cast<uintptr_t>(curr)))
        //     sleep(sleep_time);

        return true;
    }

private:
    std::atomic<T*> head { nullptr };
};

template <class T, size_t sleep_time>
bool operator==(typename List<T, sleep_time>::read_iterator& a,
                typename List<T, sleep_time>::read_iterator& b)
{
    return a->curr == b->curr;
}

template <class T, size_t sleep_time>
bool operator!=(typename List<T, sleep_time>::read_iterator& a,
                typename List<T, sleep_time>::read_iterator& b)
{
    return !operator==(a, b);
}

}

#endif
