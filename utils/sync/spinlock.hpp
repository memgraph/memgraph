#ifndef MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP
#define MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP

#include <atomic>
#include <unistd.h>

class SpinLock
{
public:
    void acquire()
    {
        while(lock.test_and_set(std::memory_order_acquire))
            usleep(250);
    }

    void release()
    {
        lock.clear(std::memory_order_release);
    }

private:
    std::atomic_flag lock;
};

#endif
