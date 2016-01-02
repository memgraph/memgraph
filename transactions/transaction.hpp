#pragma once

#include <cstdlib>
#include <cstdint>
#include <vector>

#include "mvcc/id.hpp"
#include "snapshot.hpp"
#include "lock_store.hpp"
#include "storage/locking/record_lock.hpp"

namespace tx
{

class Engine;

class Transaction
{
    friend class Engine;

public:
    Transaction(const Id& id, const Snapshot<Id>& snapshot, Engine& engine)
        : id(id), cid(1), snapshot(snapshot), engine(engine) {}

    Transaction(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;

    // index of this transaction
    const Id id;

    // index of the current command in the current transaction;
    uint8_t cid;

    // a snapshot of currently active transactions
    const Snapshot<Id> snapshot;

    void take_lock(RecordLock& lock)
    {
        locks.take(&lock, id);
    }

    // convenience methods which call the corresponging methods in the
    // transaction engine
    void commit();
    void abort();

private:
    Engine& engine;
    LockStore<RecordLock> locks;
};

}
