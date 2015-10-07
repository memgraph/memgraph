#ifndef MEMGRAPH_TRANSACTIONS_SNAPSHOT_HPP
#define MEMGRAPH_TRANSACTIONS_SNAPSHOT_HPP

#include <vector>
#include <algorithm>

namespace tx
{

template <class id_t>
class Snapshot
{
public:
    Snapshot(std::vector<id_t> active) : active(std::move(active)) {}

    Snapshot(const Snapshot& other)
    {
        active = other.active;
    }

    Snapshot(Snapshot&& other)
    {
        active = std::move(other.active);
    }

    bool is_active(id_t xid) const
    {
        return std::binary_search(active.begin(), active.end(), xid);
    }

private:
    std::vector<id_t> active;
};

}

#endif
