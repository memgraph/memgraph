#pragma once

#include <memory>
#include <vector>
#include "glog/logging.h"
#include "storage/locking/lock_status.hpp"
#include "storage/locking/record_lock.hpp"
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
    locks_.emplace_back(LockHolder(lock, tx, engine));
    if (!locks_.back().active()) {
      locks_.pop_back();
    }
  }

 private:
  std::vector<LockHolder> locks_;
};
}  // namespace tx
