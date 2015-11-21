#pragma once

#include <cstdlib>
#include <cstdint>
#include <vector>

#include "mvcc/id.hpp"
#include "snapshot.hpp"

namespace tx
{

struct Transaction
{
    Transaction(const Id& id, Snapshot<Id> snapshot)
        : id(id), cid(1), snapshot(std::move(snapshot)) {}

    // index of this transaction
    Id id;

    // index of the current command in the current transaction;
    uint8_t cid;

    // a snapshot of currently active transactions
    Snapshot<Id> snapshot;
};

}
