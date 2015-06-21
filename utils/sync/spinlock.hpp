#ifndef MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP
#define MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP

#include <atomic>
#include <unistd.h>

class SpinLock
{
public:
    void acquire();
    void release();

private:
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
};

void SpinLock::acquire()
{
    while(lock.test_and_set(std::memory_order_acquire))
        usleep(250);
}

void SpinLock::release()
{
    lock.clear(std::memory_order_release);
}

#endif
