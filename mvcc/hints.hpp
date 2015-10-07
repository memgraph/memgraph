#ifndef MEMGRAPH_MVCC_HINTS_HPP
#define MEMGRAPH_MVCC_HINTS_HPP

#include <atomic>
#include <unistd.h>

#include "transactions/commit_log.hpp"

namespace mvcc
{

// known committed and known aborted for both xmax and xmin
// this hints are used to quickly check the commit/abort status of the
// transaction that created this record. if these are not set, one should
// consult the commit log to find the status and update the status here
// more info https://wiki.postgresql.org/wiki/Hint_Bits
class Hints
{
public:
    union HintBits;

private:
    enum Flags {
        MIN_CMT = 1,      // XX01
        MIN_ABT = 1 << 1, // XX10
        MAX_CMT = 1 << 2, // 01XX
        MAX_ABT = 1 << 3  // 10XX
    };

    template <Flags COMMITTED, Flags ABORTED>
    class TxHints
    {
        using type = TxHints<COMMITTED, ABORTED>;

    public:
        TxHints(std::atomic<uint8_t>& bits)
            : bits(bits) {}

        struct Value
        {
            bool is_committed() const
            {
                return bits & COMMITTED;
            }

            bool is_aborted() const
            {
                return bits & ABORTED;
            }

            bool is_unknown() const
            {
                return !(is_committed() || is_aborted());
            }
            
            uint8_t bits;
        };

        Value load(std::memory_order order = std::memory_order_seq_cst)
        {
            return Value { bits.load(order) };
        }

        void set_committed()
        {
            bits.fetch_or(COMMITTED);
        }

        void set_aborted()
        {
            bits.fetch_or(ABORTED);
        }

    private:
        std::atomic<uint8_t>& bits;
    };

    struct Min : public TxHints<MIN_CMT, MIN_ABT>
    {
        using TxHints::TxHints;
    };

    struct Max : public TxHints<MAX_CMT, MAX_ABT>
    {
        using TxHints::TxHints;
    };

public:
    Hints() : min(bits), max(bits)
    {
        assert(bits.is_lock_free());
    }

    union HintBits
    {
        uint8_t bits;

        Min::Value min;
        Max::Value max;
    };

    HintBits load(std::memory_order order = std::memory_order_seq_cst)
    {
        return HintBits { bits.load(order) };
    }

    Min min;
    Max max;

    std::atomic<uint8_t> bits { 0 };
};

}

#endif
