#pragma once

#include "transactions/type.hpp"
#include "storage/locking/lock_status.hpp"
#include "threading/sync/futex.hpp"

class RecordLock {
  // TODO arbitrary constant, reconsider
  static constexpr struct timespec timeout { 2, 0 };

 public:
  LockStatus lock(tx::transaction_id_t id);
  void lock();
  void unlock();

 private:
  Futex mutex;
  tx::transaction_id_t owner;
};
