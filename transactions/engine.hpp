#ifndef MEMGRAPH_TRANSACTIONS_ENGINE_HPP
#define MEMGRAPH_TRANSACTIONS_ENGINE_HPP

#include <atomic>
#include <vector>

#include "transaction.hpp"
#include "transaction_cache.hpp"
#include "commit_log.hpp"

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

class Engine : Lockable<SpinLock>
{
public:
    using sptr = std::shared_ptr<Engine>;

    Engine() : counter(0) {}
    
    const Transaction& begin()
    {
        auto guard = this->acquire_unique();

        auto id = counter.next();
        auto t = new Transaction(id, active);

        active.push_back(id);
        cache.put(id, t);

        return *t;
    }

    const Transaction& advance(uint64_t id)
    {
        auto guard = this->acquire_unique();

        auto* t = cache.get(id);

        if(t == nullptr)
            throw TransactionError("transaction does not exist");

        // this is a new command
        t->cid++;

        return *t;
    }

    void commit(const Transaction& t)
    {
        auto guard = this->acquire_unique();
        CommitLog::get().set_committed(t.id);

        finalize(t);
    }

    void abort(const Transaction& t)
    {
        auto guard = this->acquire_unique();
        CommitLog::get().set_aborted(t.id);
        
        finalize(t);
    }

    uint64_t last_known_active()
    {
        auto guard = this->acquire_unique();
        return active.front();
    }

    // total number of transactions started from the beginning of time
    uint64_t count()
    {
        auto guard = this->acquire_unique();
        return counter.count();
    }

    // the number of currently active transactions
    size_t size()
    {
        auto guard = this->acquire_unique();
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
