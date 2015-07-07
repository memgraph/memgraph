#ifndef MEMGRAPH_STORAGE_RECORD_HPP
#define MEMGRAPH_STORAGE_RECORD_HPP

#include <mutex>

#include "sync/spinlock.hpp"

template <class Derived,
          class xid_t,
          class lock_t>
class Record
{
public:
    Record(uint64_t id = 0) : id(id), xmin_(0), xmax_(0), cmax_(0), cmin_(0),
        newer_(nullptr) {}

    using record_t = Record<xid_t, lock_t, Derived>;

    // every node has a unique id. 2^64 = 1.8 x 10^19. that should be enough
    // for a looong time :) but keep in mind that some vacuuming would be nice
    // to reuse indices for deleted nodes.
    uint64_t id;

    // acquire an exclusive guard on this node, used by mvcc to guard
    // concurrent writes to this record (update - update, update - delete)
    std::unique_lock<lock_t> guard()
    {
        return std::unique_lock<lock_t>(lock);
    }

    xid_t xmin()
    {
        return xmin_.load(std::memory_order_relaxed);
    }

    void xmin(xid_t value)
    {
        xmin_.store(value, std::memory_order_relaxed);
    }

    xid_t xmax()
    {
        return xmax_.load(std::memory_order_relaxed);
    }

    void xmax(xid_t value)
    {
        return xmax_.store(value, std::memory_order_relaxed);
    }

    uint8_t cmin()
    {
        return cmin_.load(std::memory_order_relaxed);
    }

    void cmin(uint8_t value)
    {
        cmin_.store(value, std::memory_order_relaxed);
    }

    uint8_t cmax()
    {
        return cmax_.load(std::memory_order_relaxed);
    }

    void cmax(uint8_t value)
    {
        return cmax_.store(value, std::memory_order_relaxed);
    }

    record_t* newer()
    {
        return newer_.load(std::memory_order_relaxed);
    }

    void newer(record_t* value)
    {
        newer_.store(value, std::memory_order_relaxed);
    }

    Derived& derived()
    {
        return *static_cast<Derived*>(this);
    }

private:
    // used by MVCC to keep track of what's visible to transactions
    std::atomic<xid_t> xmin_, xmax_;
    std::atomic<uint8_t>  cmin_, cmax_;

    std::atomic<record_t*> newer_;

    lock_t lock;
};

#endif
