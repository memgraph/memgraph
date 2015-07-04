#ifndef MEMGRAPH_TRANSACTION_TRANSACTION_HPP
#define MEMGRAPH_TRANSACTION_TRANSACTION_HPP

#include <cstdlib>
#include <vector>

template <class id_t>
struct Transaction
{
    Transaction(id_t id, std::vector<id_t> active)
        : id(id), active(std::move(active)) {}

    // index of this transaction
    id_t id;

    // the ids of the currently active transactions used by the mvcc
    // implementation for snapshot isolation
    std::vector<id_t> active;

    bool operator<(const Transaction<id_t>& rhs)
    {
        return id < rhs.id; 
    }
};

#endif
