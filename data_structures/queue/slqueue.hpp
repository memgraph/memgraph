#ifndef MEMGRAPH_DATA_STRUCTURES_QUEUE_SLQUEUE_HPP
#define MEMGRAPH_DATA_STRUCTURES_QUEUE_SLQUEUE_HPP

#include <queue>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

namespace spinlock
{

template <class T>
class Queue : Lockable<SpinLock>
{
public:

    template <class... Args>
    void emplace(Args&&... args)
    {
        auto guard = acquire();
        queue.emplace(args...);
    }

    void push(const T& item)
    {
        auto guard = acquire();
        queue.push(item);
    }

    T front()
    {
        auto guard = acquire();
        return queue.front();
    }

    void pop()
    {
        auto guard = acquire();
        queue.pop();
    }

    bool pop(T& item)
    {
        auto guard = acquire();
        if(queue.empty())
            return false;

        item = std::move(queue.front());
        queue.pop();
        return true;
    }

    bool empty()
    {
        auto guard = acquire();
        return queue.empty();
    }

    size_t size()
    {
        auto guard = acquire();
        return queue.size();
    }

private:
    std::queue<T> queue;
};

}

#endif
