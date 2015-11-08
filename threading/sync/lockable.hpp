#ifndef MEMGRAPH_SYNC_LOCKABLE_HPP
#define MEMGRAPH_SYNC_LOCKABLE_HPP

#include <mutex>
#include "spinlock.hpp"

template <class lock_t = SpinLock>
class Lockable
{
public:
    using lock_type = lock_t;

    std::lock_guard<lock_t> acquire_guard() const
    {
        return std::lock_guard<lock_t>(lock);
    }

    std::unique_lock<lock_t> acquire_unique() const
    {
        return std::unique_lock<lock_t>(lock);
    }

    mutable lock_t lock;
};

#endif
