#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "storage/locking/record_lock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"
#include "type.hpp"

namespace tx {

/** A database transaction. Encapsulates an atomic,
 * abortable unit of work. Also defines that all db
 * ops are single-threaded within a single transaction */
class Transaction {
 public:
  /** Returns the maximum possible transcation id */
  static transaction_id_t MaxId() {
    return std::numeric_limits<transaction_id_t>::max();
  }

 private:
  friend class Engine;

  // the constructor is private, only the Engine ever uses it
  Transaction(transaction_id_t id, const Snapshot &snapshot, Engine &engine);

  // a transaction can't be moved nor copied. it's owned by the transaction
  // engine, and it's lifetime is managed by it
  Transaction(const Transaction &) = delete;
  Transaction(Transaction &&) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&) = delete;

 public:
  /** Acquires the lock over the given RecordLock, preventing
   * other transactions from doing the same */
  void TakeLock(RecordLock &lock);

  /** Commits this transaction. After this call this transaction
   * object is no longer valid for use (it gets deleted by the
   * engine that owns it). */
  void Commit();

  /** Aborts this transaction. After this call this transaction
   * object is no longer valid for use (it gets deleted by the
   * engine that owns it). */
  void Abort();

  /** Transaction's id. Unique in the engine that owns it */
  const transaction_id_t id_;

  /** The transaction engine to which this transaction belongs */
  Engine &engine_;

  /** Returns the current transaction's current command id */
  // TODO rename to cmd_id (variable and function
  auto cid() const { return cid_; }

  /** Returns this transaction's snapshot. */
  const Snapshot &snapshot() const { return snapshot_; }

 private:
  // index of the current command in the current transaction;
  command_id_t cid_{1};
  // a snapshot of currently active transactions
  const Snapshot snapshot_;
  // locks
  LockStore<RecordLock> locks_;
};
}
