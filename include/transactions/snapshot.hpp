#pragma once

#include <vector>
#include <algorithm>

namespace tx
{

template <class id_t>
class Snapshot
{
public:
    Snapshot() = default;

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

    void insert(const id_t& id)
    {
        active.push_back(id);
    }

    void remove(const id_t& id)
    {
        // remove transaction from the active transactions list
        auto last = std::remove(active.begin(), active.end(), id);
        active.erase(last, active.end());
    }

    const id_t& front()
    {
        return active.front();
    }

    size_t size()
    {
        return active.size();
    }

private:
    std::vector<id_t> active;
};

}
