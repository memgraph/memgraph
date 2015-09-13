#ifndef MEMGRAPH_DATA_STRUCTURES_LIST_LOCKFREE_LIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_LIST_LOCKFREE_LIST_HPP

#include <atomic>
#include <memory>
#include <unistd.h>

namespace lockfree
{

template <class T, size_t sleep_time = 250>
class List
{
    struct Node
    {
        T item;
        std::shared_ptr<Node> next;
    };

public:
    List() = default;
    
    // the default destructor is recursive so it could blow the stack if the
    // list is long enough. the head node is destructed first via a shared_ptr
    // and then it automatically destructs the second node and the second
    // destructs the third and so on. keep that in mind
    ~List() = default;

    List(List&) = delete;
    void operator=(List&) = delete;

    class iterator
    {
    public:
        iterator(std::shared_ptr<Node> ptr) : ptr(ptr) {}

        T& operator*() { return ptr->item; }
        T* operator->() { return &ptr->item; }

        iterator& operator++()
        {
            if(ptr)
                ptr = ptr->next;

            return *this;
        }
  
        iterator& operator++(int)
        {
            operator++();
            return *this;
        }

    private:
        std::shared_ptr<Node> ptr;
    };

    iterator begin() { return iterator(std::move(head.load())); }
    iterator end() { return iterator(nullptr); }

    auto find(T item)
    {
        auto p = head.load();

        while(p && p->item != item)
            p = p->next;

        return iterator(std::move(p));
    }

    iterator push_front(T item)
    {
        auto p = std::make_shared<Node>();
        p->item = item;
        p->next = std::move(head.load());

        while(!head.compare_exchange_weak(p->next, p))
            usleep(sleep_time);

        return iterator(p);
    }

    iterator pop_front()
    {
        auto p = head.load();
        auto q = p;

        while(p && !head.compare_exchange_weak(p, p->next))
        {
            q = p;
            usleep(sleep_time);
        }

        return iterator(q);
    }

    // TODO think about how to make this lock free and safe from ABA
    // this can easily be thread safe if there is ONLY ONE concurrent
    // remove operation
    //bool remove(T item);
   
private:
    std::atomic<std::shared_ptr<Node>> head { nullptr };
};

};

#endif
