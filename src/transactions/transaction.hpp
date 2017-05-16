
#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "mvcc/id.hpp"
#include "storage/locking/record_lock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"

namespace tx {

class Engine;

class Transaction {
  friend class Engine;

 public:
  explicit Transaction(Engine &engine);
  Transaction(const Id &&id, const Snapshot<Id> &&snapshot, Engine &engine);
  Transaction(const Id &id, const Snapshot<Id> &snapshot, Engine &engine);
  Transaction(const Transaction &) = delete;
  Transaction(Transaction &&) = default;

  // Blocks until all transactions from snapshot finish, except the 'id' one.
  // After this method, snapshot will be either empty or contain transaction
  // with Id 'id'.
  void wait_for_active_except(const Id &id) const;

  void take_lock(RecordLock &lock);
  void commit();
  void abort();

  // True if this transaction and every transaction from snapshot have
  // finished.
  bool all_finished();

  // Return id of oldest transaction from snapshot.
  Id oldest_active();

  // True if id is in snapshot.
  bool in_snapshot(const Id &id) const;

  // index of this transaction
  const Id id;

  // index of the current command in the current transaction;
  uint64_t cid;

  Engine &engine;

 private:
  // a snapshot of currently active transactions
  const Snapshot<Id> snapshot;
  LockStore<RecordLock> locks;
};
}
