#ifndef MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP
#define MEMGRAPH_UTILS_SYNC_SPINLOCK_HPP

#include <atomic>
#include <unistd.h>

class SpinLock
{
public:

    void lock()
    {
        // TODO add asm_pause and counter first before sleeping
        // might be faster, but test this and see
        while(lock_flag.test_and_set(std::memory_order_acquire))
            usleep(250);
    }

    void unlock()
    {
        lock_flag.clear(std::memory_order_release);
    }

private:
    // guaranteed by standard to be lock free!
    std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;
};

#endif
