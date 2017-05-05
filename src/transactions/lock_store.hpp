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
    LockHolder() noexcept = default;

    template <class... Args>
    LockHolder(T *lock, Args &&... args) : lock(lock) {
      debug_assert(lock != nullptr, "Lock is nullptr.");
      auto status = lock->lock(std::forward<Args>(args)...);

      if (status != LockStatus::Acquired) lock = nullptr;
    }

    LockHolder(const LockHolder &) = delete;
    LockHolder(LockHolder &&other) noexcept : lock(other.lock) {
      other.lock = nullptr;
    }

    ~LockHolder() {
      if (lock != nullptr) lock->unlock();
    }

    bool active() const noexcept { return lock != nullptr; }

   private:
    T *lock{nullptr};
  };

 public:
  template <class... Args>
  void take(T *lock, Args &&... args) {
    locks.emplace_back(LockHolder(lock, std::forward<Args>(args)...));
    if (!locks.back().active()) {
      locks.pop_back();
      return;
    }
  }

 private:
  std::vector<LockHolder> locks;
};
};
