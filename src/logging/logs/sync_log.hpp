#pragma once

#include "logging/log.hpp"
#include "threading/sync/futex.hpp"
#include "threading/sync/lockable.hpp"

class SyncLog : public Log, Lockable<Futex> {
 protected:
  void emit(Record::uptr) override;
  std::string type() override;
};
