#ifndef MEMGRAPH_MVCC_TRANSACTIONENGINE_HPP
#define MEMGRAPH_MVCC_TRANSACTIONENGINE_HPP

#include <cstdlib>
#include <atomic>
#include <mutex>
#include <vector>
#include <set>
#include <list>
#include <algorithm>

#include "transaction.hpp"
#include "transaction_cache.hpp"

#include "utils/counters/simple_counter.hpp"

#include "threading/sync/spinlock.hpp"
#include "threading/sync/lockable.hpp"

namespace tx
{

class TransactionError : std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

class TransactionEngine : Lockable<SpinLock>
{
public:
    TransactionEngine(uint64_t n) : counter(n) {}
    
    const Transaction& begin()
    {
        auto guard = this->acquire();

        auto id = counter.next();
        auto t = new Transaction(id, active);

        active.push_back(id);
        cache.put(id, t);

        return *t;
    }

    const Transaction& advance(uint64_t id)
    {
        auto guard = this->acquire();

        auto* t = cache.get(id);

        if(t == nullptr)
            throw TransactionError("transaction does not exist");

        // this is a new command
        t->cid++;

        return *t;
    }

    void commit(const Transaction& t)
    {
        auto guard = this->acquire();

        finalize(t);
    }

    void rollback(const Transaction& t)
    {
        auto guard = this->acquire();
        
        finalize(t);
    }

    uint64_t last_known_active()
    {
        auto guard = this->acquire();
        return active.front();
    }

    // total number of transactions started from the beginning of time
    uint64_t count()
    {
        auto guard = this->acquire();
        return counter.count();
    }

    // the number of currently active transactions
    size_t size()
    {
        auto guard = this->acquire();
        return active.size();
    }

private:
    void finalize(const Transaction& t)
    {
        // remove transaction from the active transactions list
        auto last = std::remove(active.begin(), active.end(), t.id);
        active.erase(last, active.end());

        // remove transaction from cache
        cache.del(t.id);
    }

    SimpleCounter<uint64_t> counter;

    std::vector<uint64_t> active;
    TransactionCache<uint64_t> cache;
};

}

#endif
