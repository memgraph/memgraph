#ifndef MEMGRAPH_UTILS_SYNC_CASLOCK_HPP
#define MEMGRAPH_UTILS_SYNC_CASLOCK_HPP

#include <atomic>

struct CasLock
{
    uint8_t lock;

    void lock()
    {
    }

    void unlock()
    {
    }
    
};

#endif
