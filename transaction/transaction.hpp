#ifndef MEMGRAPH_TRANSACTION_TRANSACTION_HPP
#define MEMGRAPH_TRANSACTION_TRANSACTION_HPP

#include <cstdlib>
#include <cstdint>
#include <vector>

#include "transaction/commit_log.hpp"

struct Transaction
{
    Transaction(uint64_t id, std::vector<uint64_t> active)
        : id(id), cid(1), active(std::move(active)) {}

    // index of this transaction
    uint64_t id;

    // index of the current command in the current transaction;
    uint8_t cid;

    // the ids of the currently active transactions used by the mvcc
    // implementation for snapshot transaction isolation.
    // std::vector is much faster than std::set for fewer number of items
    // we don't expect the number of active transactions getting too large.
    std::vector<uint64_t> active;

    // check weather the transaction with the xid looks committed from the
    // database snapshot given to this transaction
    bool committed(uint64_t xid) const
    {
        // transaction xid is newer than id and therefore not visible at all
        if (xid > id)
            return false;

        // transaction xid is not visible if it's currently active. the
        // active transactions are sorted ascending and therefore we can stop
        // looking as soon as we hit the active transaction with id greater
        // than xid
        for(size_t i = 0; i < active.size(); ++i)
            if(xid <= active[i])
                return false;

        return true;
    }
};

#endif
