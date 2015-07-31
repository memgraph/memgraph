#ifndef MEMGRAPH_SYNC_LOCKABLE_HPP
#define MEMGRAPH_SYNC_LOCKABLE_HPP

#include <mutex>
#include "spinlock.hpp"

template <class lock_t = SpinLock>
class Lockable
{
protected:
    std::unique_lock<lock_t> acquire()
    {
        return std::unique_lock<lock_t>(lock);
    }
    
    lock_t lock;
};

#endif
