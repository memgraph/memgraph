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
#include "utils/counters/simple_counter.hpp"

#include "threading/sync/spinlock.hpp"
#include "threading/sync/lockable.hpp"

class TransactionEngine : Lockable<SpinLock>
{
public:
    TransactionEngine(uint64_t n) : counter(n) {}
    
    Transaction begin()
    {
        auto guard = this->acquire();

        auto id = counter.next();
        auto t = Transaction(id, active);

        active.push_back(id);

        return t;
    }

    void commit(const Transaction& t)
    {
        auto guard = this->acquire();

        finalize(t);
    }

    void rollback(const Transaction& t)
    {
        auto guard = this->acquire();
        // what to do here?
        
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
        auto x = t.id;

        // remove transaction from the active transactions list
        auto last = std::remove(active.begin(), active.end(), x);
        active.erase(last, active.end());
    }

    SimpleCounter<uint64_t> counter;

    std::vector<uint64_t> active;
};

#endif
