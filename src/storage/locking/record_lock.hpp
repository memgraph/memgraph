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
};

class RecordLock {
 public:
  LockStatus Lock(const tx::Transaction &id, tx::Engine &engine);

  void Unlock();

 private:
  // Arbitrary choosen constant, postgresql uses 1 second so do we.
  constexpr static std::chrono::duration<double> kTimeout{
      std::chrono::seconds(1)};

  // TODO: Because of the current architecture it is somewhat OK to use SpinLock
  // here. Once we reimplement worker architecture to execute some other
  // transaction in this thread while other is waiting for a lock this will had
  // to change to something else.
  SpinLock lock_;
  std::atomic<tx::transaction_id_t> owner_{0};
};
