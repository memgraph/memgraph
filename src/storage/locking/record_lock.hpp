#pragma once

#include "mvcc/id.hpp"
#include "storage/locking/lock_status.hpp"
#include "threading/sync/futex.hpp"

class RecordLock {
  static constexpr struct timespec timeout { 20, 0 };
  static constexpr Id INVALID = Id();

 public:
  LockStatus lock(const Id& id);
  void lock();
  void unlock();

 private:
  Futex mutex;
  Id owner;
};
