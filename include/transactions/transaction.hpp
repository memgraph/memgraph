
#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "mvcc/id.hpp"
#include "storage/locking/record_lock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/transaction_id.hpp"

namespace tx
{

class Transaction : public TransactionId
{

public:
    Transaction(const Id &id, const Snapshot<Id> &snapshot, Engine &engine);
    Transaction(const Transaction &) = delete;
    Transaction(Transaction &&) = delete;

    // Returns copy of transaction_id
    TransactionId transaction_id();

    // Blocks until all transactions from snapshot finish. After this method,
    // snapshot will be empty.
    void wait_for_active();

    void take_lock(RecordLock &lock);
    void commit();
    void abort();

private:
    LockStore<RecordLock> locks;
};
}
