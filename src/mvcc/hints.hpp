#pragma once

#include <unistd.h>
#include <atomic>

#include "transactions/commit_log.hpp"
#include "utils/assert.hpp"

namespace mvcc {

// known committed and known aborted for both cre and exp
// this hints are used to quickly check the commit/abort status of the
// transaction that created this record. if these are not set, one should
// consult the commit log to find the status and update the status here
// more info https://wiki.postgresql.org/wiki/Hint_Bits
class Hints {
 public:
  union HintBits;

 private:
  enum Flags : uint8_t {
    CRE_CMT = 0x01,  // __01
    CRE_ABT = 0x02,  // __10
    EXP_CMT = 0x04,  // 01__
    EXP_ABT = 0x08   // 10__
  };

  template <Flags COMMITTED, Flags ABORTED>
  class TxHints {
    using type = TxHints<COMMITTED, ABORTED>;

   public:
    TxHints(std::atomic<uint8_t> &bits) : bits(bits) {}

    struct Value {
      bool is_committed() const { return bits & COMMITTED; }

      bool is_aborted() const { return bits & ABORTED; }

      bool is_unknown() const { return !(is_committed() || is_aborted()); }

      uint8_t bits;
    };

    Value load(std::memory_order order = std::memory_order_seq_cst) {
      return Value{bits.load(order)};
    }

    void set_committed(std::memory_order order = std::memory_order_seq_cst) {
      bits.fetch_or(COMMITTED, order);
    }

    void set_aborted(std::memory_order order = std::memory_order_seq_cst) {
      bits.fetch_or(ABORTED, order);
    }

   private:
    std::atomic<uint8_t> &bits;
  };

  struct Cre : public TxHints<CRE_CMT, CRE_ABT> {
    using TxHints::TxHints;
  };

  struct Exp : public TxHints<EXP_CMT, EXP_ABT> {
    using TxHints::TxHints;
  };

 public:
  Hints() : cre(bits), exp(bits) {
    debug_assert(bits.is_lock_free(), "Bits are not lock free.");
  }

  union HintBits {
    uint8_t bits;

    Cre::Value cre;
    Exp::Value exp;
  };

  HintBits load(std::memory_order order = std::memory_order_seq_cst) {
    return HintBits{bits.load(order)};
  }

  Cre cre;
  Exp exp;

  std::atomic<uint8_t> bits{0};
};
}
