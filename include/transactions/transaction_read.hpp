
#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "mvcc/id.hpp"
#include "transactions/snapshot.hpp"

namespace tx
{

class Engine;

// Has read only capbilities.
// TODO: Not all applicable methods in code have been changed to accept
// TransactionRead instead of a Transaction.
class TransactionRead
{
    friend class Engine;

public:
    TransactionRead(Engine &engine);

    TransactionRead(const Id &&id, const Snapshot<Id> &&snapshot,
                    Engine &engine);

    TransactionRead(const Id &id, const Snapshot<Id> &snapshot, Engine &engine);

    // Return id of oldest transaction from snapshot.
    Id oldest_active();

    // True if id is in snapshot.
    bool in_snapshot(const Id &id) const;

    // index of this transaction
    const Id id;

    // index of the current command in the current transaction;
    uint8_t cid;

    Engine &engine;

protected:
    // a snapshot of currently active transactions
    Snapshot<Id> snapshot;
};
}
