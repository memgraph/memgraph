#ifndef MEMGRAPH_STORAGE_RECORD_HPP
#define MEMGRAPH_STORAGE_RECORD_HPP

#include <mutex>
#include <list>

#include "sync/spinlock.hpp"

template <class lock_type = SpinLock>
class Record
{
    // every node has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes. also, vacuuming would enable the
    // use of uint32_t which could be more memory conserving
    uint64_t id;

    // acquire an exclusive guard on this node, use for concurrent access
    std::unique_lock<lock_type> guard()
    {
        return std::unique_lock<lock_type>(lock);
    }

    uint64_t xmin()
    {
        return xmin_.load(std::memory_order_acquire);
    }

    uint64_t xmin_inc()
    {
        return xmin_.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t xmax()
    {
        return xmax_.load(std::memory_order_release);
    }

    uint64_t xmax_inc()
    {
        return xmax_.fetch_add(1, std::memory_order_relaxed);
    }

    Record* newer()
    {
        return versions.load(std::memory_order_consume);
    }

private:
    // used by MVCC to keep track of what's visible to transactions
    std::atomic<uint64_t> xmin_, xmax_;

    std::atomic<Record*> versions;

    lock_type lock;
};

#endif
