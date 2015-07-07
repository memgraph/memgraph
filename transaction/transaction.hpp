#ifndef MEMGRAPH_TRANSACTION_TRANSACTION_HPP
#define MEMGRAPH_TRANSACTION_TRANSACTION_HPP

#include <cstdlib>
#include <vector>

template <class id_t>
struct Transaction
{
    Transaction(id_t id, std::vector<id_t> active)
        : id(id), cid(1), active(std::move(active)) {}

    // index of this transaction
    id_t id;

    // index of the current command in the current transaction;
    uint8_t cid;

    // the ids of the currently active transactions used by the mvcc
    // implementation for snapshot transaction isolation.
    // std::vector is much faster than std::set for fewer number of items
    // we don't expect the number of active transactions getting too large.
    std::vector<id_t> active;

    // check weather the transaction with the xid looks committed from the
    // database snapshot given to this transaction
    bool committed(id_t xid)
    {
        for(size_t i = 0; i < active.size(); ++i)
            if(xid < active[i])
                return false;

        return true;
    }
};

#endif
