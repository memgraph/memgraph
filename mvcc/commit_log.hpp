#ifndef MEMGRAPH_TRANSACTION_COMMIT_LOG_HPP
#define MEMGRAPH_TRANSACTION_COMMIT_LOG_HPP

#include <cstdint>

#include <boost/dynamic_bitset/dynamic_bitset.hpp>

#include "threading/sync/spinlock.hpp"
#include "threading/sync/lockable.hpp"

// optimize allocation performance by preallocating chunks in like 2MB or so
// optimize memory by purging old transactions after vacuuming or something
// this is not sustainable in the long run

class CommitLog : Lockable<SpinLock>
{
    void begin()
    {
        auto guard = this->acquire();
        log.push_back(0);
    }

    void mark_aborted(uint64_t)
    {
        // this would be useful in a different implementation
    }

    void mark_committed(uint64_t id)
    {
        auto guard = this->acquire();
        log.set(id, true);
    }

    bool is_committed(uint64_t id)
    {
        auto guard = this->acquire();
        return log[id];
    }

private:
    boost::dynamic_bitset<uint64_t> log;
};

#endif
