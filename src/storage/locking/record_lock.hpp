#pragma once

#include <atomic>
#include <chrono>
#include <unordered_set>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/locking/lock_status.hpp"
#include "threading/sync/futex.hpp"
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
  bool TryLock(tx::transaction_id_t tx_id);

  // Arbitrary choosen constant, postgresql uses 1 second so do we.
  constexpr static std::chrono::duration<double> kTimeout{
      std::chrono::seconds(1)};

  std::atomic<tx::transaction_id_t> owner_{0};
};
