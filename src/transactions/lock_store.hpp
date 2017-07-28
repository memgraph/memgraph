#pragma once

#include <memory>
#include <vector>
#include "storage/locking/lock_status.hpp"
#include "utils/assert.hpp"

namespace tx {

template <class T>
class LockStore {
  class LockHolder {
   public:
    LockHolder() = default;

    template <class... Args>
    LockHolder(T *lock, Args &&... args) : lock_(lock) {
      debug_assert(lock != nullptr, "Lock is nullptr.");
      auto status = lock_->Lock(std::forward<Args>(args)...);

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
    }

    ~LockHolder() {
      if (lock_ != nullptr) {
        lock_->Unlock();
      }
    }

    bool active() const { return lock_ != nullptr; }

   private:
    T *lock_{nullptr};
  };

 public:
  template <class... Args>
  void Take(T *lock, Args &&... args) {
    locks_.emplace_back(LockHolder(lock, std::forward<Args>(args)...));
    if (!locks_.back().active()) {
      locks_.pop_back();
    }
  }

 private:
  std::vector<LockHolder> locks_;
};
}
