#ifndef MEMGRAPH_MVCC_TRANSACTION_HPP
#define MEMGRAPH_MVCC_TRANSACTION_HPP

#include <cstdlib>
#include <cstdint>
#include <vector>

#include "snapshot.hpp"

namespace tx
{

struct Transaction
{
    Transaction(uint64_t id, Snapshot<uint64_t> snapshot)
        : id(id), cid(1), snapshot(std::move(snapshot)) {}

    // index of this transaction
    uint64_t id;

    // index of the current command in the current transaction;
    uint8_t cid;

    // a snapshot of currently active transactions
    Snapshot<uint64_t> snapshot;
};

}

#endif
