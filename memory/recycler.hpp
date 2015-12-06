#pragma once

#include <memory>
#include <queue>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <class T, class Allocator=std::allocator<T>>
class Recycler : Lockable<SpinLock>
{
    static constexpr size_t default_max_reserved = 100;

public:
    Recycler() = default;
    Recycler(size_t max_reserved) : max_reserved(max_reserved) {}

    template <class... Args>
    T* acquire(Args&&... args)
    {
        auto guard = acquire_unique();
        return new T(std::forward<Args>(args)...); // todo refactor :D
    }

    void release(T* item)
    {
        auto guard = acquire_unique();
        delete item; // todo refactor :D
    }

private:
    Allocator alloc;
    size_t max_reserved {default_max_reserved};
    std::queue<T*> items;
};
