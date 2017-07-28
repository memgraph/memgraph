#pragma once

#include "storage/locking/lock_status.hpp"
#include "threading/sync/futex.hpp"
#include "transactions/type.hpp"

class RecordLock {
  // TODO arbitrary constant, reconsider
  static constexpr struct timespec kTimeout { 2, 0 };

 public:
  LockStatus Lock(tx::transaction_id_t id);
  void Unlock();

 private:
  Futex mutex_;
  tx::transaction_id_t owner_;
};
