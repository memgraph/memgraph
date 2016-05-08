#pragma once

#include "mvcc/id.hpp"
#include "data_structures/bitset/dynamic_bitset.hpp"

namespace tx
{

class CommitLog
{
public:
    struct Info
    {
        enum Status
        {
            ACTIVE    = 0, // 00
            COMMITTED = 1, // 01
            ABORTED   = 2, // 10
        };

        bool is_active() const
        {
            return flags & ACTIVE;
        }

        bool is_committed() const
        {
            return flags & COMMITTED;
        }

        bool is_aborted() const
        {
            return flags & ABORTED;
        }

        operator uint8_t() const
        {
            return flags;
        }

        uint8_t flags;
    };

    CommitLog() = default;
    CommitLog(CommitLog&) = delete;
    CommitLog(CommitLog&&) = delete;

    CommitLog operator=(CommitLog) = delete;

    Info fetch_info(const Id& id)
    {
        return Info { log.at(2 * id, 2) };
    }

    bool is_active(const Id& id)
    {
        return fetch_info(id).is_active();
    }

    bool is_committed(const Id& id)
    {
        return fetch_info(id).is_committed();
    }

    void set_committed(const Id& id)
    {
        log.set(2 * id);
    }

    bool is_aborted(const Id& id)
    {
        return fetch_info(id).is_aborted();
    }

    void set_aborted(const Id& id)
    {
        log.set(2 * id + 1);
    }

private:
    DynamicBitset<uint8_t, 32768> log;
};

}
