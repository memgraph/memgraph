#pragma once

#include <queue>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <class T>
class SlQueue : Lockable<SpinLock>
{
public:

    template <class... Args>
    void emplace(Args&&... args)
    {
        auto guard = acquire_unique();
        queue.emplace(args...);
    }

    void push(const T& item)
    {
        auto guard = acquire_unique();
        queue.push(item);
    }

    T front()
    {
        auto guard = acquire_unique();
        return queue.front();
    }

    void pop()
    {
        auto guard = acquire_unique();
        queue.pop();
    }

    bool pop(T& item)
    {
        auto guard = acquire_unique();
        if(queue.empty())
            return false;

        item = std::move(queue.front());
        queue.pop();
        return true;
    }

    bool empty()
    {
        auto guard = acquire_unique();
        return queue.empty();
    }

    size_t size()
    {
        auto guard = acquire_unique();
        return queue.size();
    }

private:
    std::queue<T> queue;
};
