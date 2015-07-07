#ifndef MEMGRAPH_TRANSACTION_TRANSACTIONENGINE_HPP
#define MEMGRAPH_TRANSACTION_TRANSACTIONENGINE_HPP

#include <cstdlib>
#include <atomic>
#include <mutex>
#include <vector>
#include <set>
#include <list>

#include "transaction.hpp"

template <class id_t, class lock_t>
class TransactionEngine
{
    using trans_t = Transaction<id_t>;

public:
    TransactionEngine(id_t n) : counter(n) {}
    
    Transaction<id_t> begin()
    {
        auto guard = acquire();

        auto id = ++counter;
        auto t = Transaction<id_t>(id, active);

        active.push_back(id);

        return t;
    }

    void commit(const Transaction<id_t>& t)
    {
        auto guard = acquire();

        finalize_transaction(t);
    }

    void rollback(const Transaction<id_t>& t)
    {
        auto guard = acquire();
        // what to do here?
        
        finalize_transaction(t);
    }

    // id of the last finished transaction
    id_t epochs_passed()
    {
        auto guard = acquire();
        return active.front() - 1;
    }

    // total number of transactions started from the beginning of time
    id_t count()
    {
        auto guard = acquire();
        return counter;
    }

    // the number of currently active transactions
    size_t size()
    {
        auto guard = acquire();
        return active.size();
    }

private:
    void finalize_transaction(const Transaction<id_t>& t)
    {
        // remove transaction from the active transactions list
        auto last = std::remove(active.begin(), active.end(), t.id);
        active.erase(last, active.end());
    }

    std::unique_lock<lock_t> acquire()
    {
        return std::unique_lock<lock_t>(lock);
    }

    id_t counter;
    lock_t lock;

    std::vector<id_t> active;
};

#endif
