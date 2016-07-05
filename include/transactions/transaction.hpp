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
    const Snapshot<Id> snapshot;

    void take_lock(RecordLock &lock);
    void commit();
    void abort();

    Engine &engine;

private:
    LockStore<RecordLock> locks;
};
}
