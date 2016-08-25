#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "mvcc/id.hpp"
#include "storage/locking/record_lock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"

namespace tx
{

class Engine;

class Transaction
{
    friend class Engine;

public:
    Transaction(const Id &id, const Snapshot<Id> &snapshot, Engine &engine);
    Transaction(const Transaction &) = delete;
    Transaction(Transaction &&) = delete;

    // index of this transaction
    const Id id;
    // index of the current command in the current transaction;
    uint8_t cid;
    // a snapshot of currently active transactions

    // Blocks until all transactions from snapshot finish. After this method,
    // snapshot will be empty.
    void wait_for_active();

    // True if id is in snapshot.
    bool is_active(const Id &id) const;
    void take_lock(RecordLock &lock);
    void commit();
    void abort();

    Engine &engine;

private:
    Snapshot<Id> snapshot;
    LockStore<RecordLock> locks;
};
}
