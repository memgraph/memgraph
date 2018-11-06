/// @file

#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/common/locking/record_lock.hpp"
#include "transactions/lock_store.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"
#include "utils/exceptions.hpp"

namespace tx {

/// Indicates an error in transaction handling (currently
/// only command id overflow).
class TransactionError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/// A database transaction. Encapsulates an atomic, abortable unit of work. Also
/// defines that all db ops are single-threaded within a single transaction
class Transaction final {
 public:
  /// Returns the maximum possible transcation id
  static TransactionId MaxId() {
    return std::numeric_limits<TransactionId>::max();
  }

 private:
  friend class Engine;

  // The constructor is private, only the Engine ever uses it.
  Transaction(TransactionId id, const Snapshot &snapshot, Engine &engine,
              bool blocking)
      : id_(id),
        engine_(engine),
        snapshot_(snapshot),
        blocking_(blocking) {}

  // A transaction can't be moved nor copied. it's owned by the transaction
  // engine, and it's lifetime is managed by it.
  Transaction(const Transaction &) = delete;
  Transaction(Transaction &&) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&) = delete;

 public:
  /// Acquires the lock over the given RecordLock, preventing other transactions
  /// from doing the same
  void TakeLock(RecordLock &lock) const { locks_.Take(&lock, *this, engine_); }

  /// Transaction's id. Unique in the engine that owns it
  const TransactionId id_;

  /// The transaction engine to which this transaction belongs
  Engine &engine_;

  /// Returns the current transaction's current command id
  // TODO rename to cmd_id (variable and function
  auto cid() const { return cid_; }

  /// Returns this transaction's snapshot.
  const Snapshot &snapshot() const { return snapshot_; }

  /// Signal to transaction that it should abort. It doesn't really enforce that
  /// transaction will abort, but it merely hints too the transaction that it is
  /// preferable to stop its execution.
  void set_should_abort() { should_abort_ = true; }

  bool should_abort() const { return should_abort_; }

  auto creation_time() const { return creation_time_; }

  auto blocking() const { return blocking_; }

 private:
  // Function used to advance the command.
  CommandId AdvanceCommand() {
    if (cid_ == std::numeric_limits<CommandId>::max()) {
      throw TransactionError(
          "Reached maximum number of commands in this "
          "transaction.");
    }
    return ++cid_;
  }

  // Function used to set the command.
  void SetCommand(CommandId cid) { cid_ = cid; }

  // Index of the current command in the current transaction.
  CommandId cid_{1};

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

  bool blocking_{false};
};
}  // namespace tx
