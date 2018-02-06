#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "glog/logging.h"
#include "storage/locking/lock_status.hpp"
#include "storage/locking/record_lock.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/type.hpp"

namespace tx {

class Engine;
class Transaction;

class LockStore {
  class LockHolder {
   public:
    LockHolder() = default;

    LockHolder(RecordLock *lock, const Transaction &tx, tx::Engine &engine)
        : lock_(lock) {
      DCHECK(lock != nullptr) << "Lock is nullptr.";
      auto status = lock_->Lock(tx, engine);

      if (status != LockStatus::Acquired) {
        lock_ = nullptr;
      }
    }

    LockHolder(const LockHolder &) = delete;
    LockHolder &operator=(const LockHolder &) = delete;

    LockHolder(LockHolder &&other) : lock_(other.lock_) {
      other.lock_ = nullptr;
    }

    LockHolder &operator=(LockHolder &&other) {
      if (this == &other) return *this;
      lock_ = other.lock_;
      other.lock_ = nullptr;
      return *this;
    }

    ~LockHolder() {
      if (lock_ != nullptr) {
        lock_->Unlock();
      }
    }

    bool active() const { return lock_ != nullptr; }

   private:
    RecordLock *lock_{nullptr};
  };

 public:
  void Take(RecordLock *lock, const tx::Transaction &tx, tx::Engine &engine) {
    // Creating a lock holder locks the version list to the given transaction.
    // Note that it's an op that can take a long time (if there are multiple
    // transactions trying to lock.
    LockHolder holder{lock, tx, engine};

    // This guard prevents the same transaction from concurrent modificaton of
    // locks_. This can only happen in distributed memgraph, when there are
    // multiple edits coming to the same worker in the same transaction at the
    // same time. IMPORTANT: This guard must come after LockHolder construction,
    // as that potentially takes a long time and this guard only needs to
    // protect locks_ update.
    std::lock_guard<SpinLock> guard{locks_lock_};
    locks_.emplace_back(std::move(holder));
    if (!locks_.back().active()) {
      locks_.pop_back();
    }
  }

 private:
  SpinLock locks_lock_;
  std::vector<LockHolder> locks_;
};
}  // namespace tx
