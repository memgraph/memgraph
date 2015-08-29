#ifndef MEMGRAPH_UTILS_SYNC_CASLOCK_HPP
#define MEMGRAPH_UTILS_SYNC_CASLOCK_HPP

#include <atomic>
#include <unistd.h>

class CasLock
{
public:

    void lock()
    {
        bool locked = false;

        while(!lock_flag.compare_exchange_weak(locked, true,
                                               std::memory_order_release,
                                               std::memory_order_relaxed))
        {
            usleep(250);
        }
    }

    void unlock()
    {
        lock_flag.store(0, std::memory_order_release);
    }

    bool locked()
    {
        return lock_flag.load(std::memory_order_relaxed);
    }

private:
    std::atomic<bool> lock_flag;
};

#endif
