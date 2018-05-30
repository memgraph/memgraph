#pragma once

#include <atomic>
#include <chrono>
#include <unordered_set>

#include "storage/locking/lock_status.hpp"
#include "transactions/type.hpp"

namespace tx {
class Engine;
class Transaction;
};  // namespace tx

class RecordLock {
 public:
  LockStatus Lock(const tx::Transaction &id, tx::Engine &engine);

  void Unlock();

 private:
  bool TryLock(tx::TransactionId tx_id);

  // Arbitrary choosen constant, postgresql uses 1 second so do we.
  constexpr static std::chrono::duration<double> kTimeout{
      std::chrono::seconds(1)};

  std::atomic<tx::TransactionId> owner_{0};
};
