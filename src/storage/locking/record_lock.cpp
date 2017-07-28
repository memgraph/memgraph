#include "storage/locking/record_lock.hpp"

LockStatus RecordLock::Lock(tx::transaction_id_t id) {
  if (mutex_.try_lock()) {
    owner_ = id;
    return LockStatus::Acquired;
  }

  if (owner_ == id) return LockStatus::AlreadyHeld;

  mutex_.lock(&kTimeout);
  return LockStatus::Acquired;
}

void RecordLock::Unlock() { mutex_.unlock(); }

constexpr struct timespec RecordLock::kTimeout;
