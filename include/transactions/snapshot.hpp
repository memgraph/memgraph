#pragma once

#include <algorithm>
#include <vector>

#include "mvcc/id.hpp"
#include "utils/option.hpp"

namespace tx
{

class Engine;

template <class id_t>
class Snapshot
{
public:
    Snapshot() = default;

    Snapshot(std::vector<id_t> active) : active(std::move(active)) {}

    Snapshot(const Snapshot &other) { active = other.active; }

    Snapshot(Snapshot &&other) { active = std::move(other.active); }

    // True if all transaction from snapshot have finished.
    bool all_finished(Engine &engine);

    bool is_active(id_t xid) const
    {
        return std::binary_search(active.begin(), active.end(), xid);
    }

    // Return id of oldest transaction. None if there is no transactions in
    // snapshot.
    Option<Id> oldest_active()
    {
        auto n = active.size();
        if (n > 0) {
            Id min = active[0];
            for (auto i = 1; i < n; i++) {
                if (active[i] < min) {
                    min = active[i];
                }
            }
            return Option<Id>(min);

        } else {
            return Option<Id>();
        }
    }

    void insert(const id_t &id) { active.push_back(id); }

    void remove(const id_t &id)
    {
        // remove transaction from the active transactions list
        auto last = std::remove(active.begin(), active.end(), id);
        active.erase(last, active.end());
    }

    const id_t &front() { return active.front(); }

    const id_t &back() { return active.back(); }

    size_t size() { return active.size(); }

private:
    std::vector<id_t> active;
};
}
