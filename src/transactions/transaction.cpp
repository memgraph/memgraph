#include "transactions/transaction.hpp"

#include <chrono> // std::chrono::seconds

#include <thread> // std::this_thread::sleep_for

#include "transactions/engine.hpp"

namespace tx
{

Transaction::Transaction(const Id &id, const Snapshot<Id> &snapshot,
                         Engine &engine)
    : TransactionRead(id, snapshot, engine)
{
}

// Returns copy of transaction_id
TransactionRead Transaction::transaction_read()
{
    TransactionRead const &t = *this;
    return t;
}

void Transaction::wait_for_active()
{
    while (snapshot.size() > 0) {
        auto sid = snapshot.back();
        while (engine.clog.fetch_info(sid).is_active()) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        snapshot.remove(sid);
    }
}

void Transaction::take_lock(RecordLock &lock) { locks.take(&lock, id); }

void Transaction::commit() { engine.commit(*this); }

void Transaction::abort() { engine.abort(*this); }
}
