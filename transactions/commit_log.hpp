#ifndef MEMGRAPH_TRANSACTIONS_COMMIT_LOG_HPP
#define MEMGRAPH_TRANSACTIONS_COMMIT_LOG_HPP

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

    static CommitLog& get()
    {
        static CommitLog log;
        return log;
    }

    Info fetch_info(uint64_t id)
    {
        return Info { log.at(2 * id, 2) };
    }

    bool is_active(uint64_t id)
    {
        return fetch_info(id).is_active();
    }

    bool is_committed(uint64_t id)
    {
        return fetch_info(id).is_committed();
    }

    void set_committed(uint64_t id)
    {
        log.set(2 * id);
    }

    bool is_aborted(uint64_t id)
    {
        return fetch_info(id).is_aborted();
    }

    void set_aborted(uint64_t id)
    {
        log.set(2 * id + 1);
    }

private:
    DynamicBitset<uint8_t, 32768> log;
};

}

#endif
