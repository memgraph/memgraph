#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/locking/record_lock.hpp"
#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"

namespace tx {

/** A database transaction. Encapsulates an atomic, abortable unit of work. Also
 * defines that all db ops are single-threaded within a single transaction */
class Transaction {
 public:
  /** Returns the maximum possible transcation id */
  static transaction_id_t MaxId() {
    return std::numeric_limits<transaction_id_t>::max();
  }

 private:
  friend class MasterEngine;
  friend class WorkerEngine;

  // The constructor is private, only the Engine ever uses it.
  Transaction(transaction_id_t id, const Snapshot &snapshot, Engine &engine)
      : id_(id), engine_(engine), snapshot_(snapshot) {}

  // A transaction can't be moved nor copied. it's owned by the transaction
  // engine, and it's lifetime is managed by it.
  Transaction(const Transaction &) = delete;
  Transaction(Transaction &&) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&) = delete;

 public:
  /** Acquires the lock over the given RecordLock, preventing other transactions
   * from doing the same */
  void TakeLock(RecordLock &lock) const { locks_.Take(&lock, *this, engine_); }

  /** Transaction's id. Unique in the engine that owns it */
  const transaction_id_t id_;

  /** The transaction engine to which this transaction belongs */
  Engine &engine_;

  /** Returns the current transaction's current command id */
  // TODO rename to cmd_id (variable and function
  auto cid() const { return cid_; }

  /** Returns this transaction's snapshot. */
  const Snapshot &snapshot() const { return snapshot_; }

  /** Signal to transaction that it should abort. It doesn't really enforce that
   * transaction will abort, but it merely hints too the transaction that it is
   * preferable to stop its execution.
   */
  void set_should_abort() { should_abort_ = true; }

  bool should_abort() const { return should_abort_; }

  auto creation_time() const { return creation_time_; }

 private:
  // Index of the current command in the current transaction.
  command_id_t cid_{1};

  // A snapshot of currently active transactions.
  const Snapshot snapshot_;

  // Record locks held by this transaction.
  mutable LockStore locks_;

  // True if transaction should abort. Used to signal query executor that it
  // should stop execution, it is only a hint, transaction can disobey.
  std::atomic<bool> should_abort_{false};

  // Creation time.
  const std::chrono::time_point<std::chrono::steady_clock> creation_time_{
      std::chrono::steady_clock::now()};
};
}  // namespace tx
