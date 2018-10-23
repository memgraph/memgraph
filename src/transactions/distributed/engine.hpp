/// @file

#pragma once

#include <algorithm>
#include <mutex>
#include <vector>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/transaction.hpp"
#include "transactions/type.hpp"

namespace tx {
/// Database transaction engine. Used for managing transactions and the related
/// information such as transaction snapshots and the transaction state info.
///
/// This is an abstract base class for implementing a transactional engine.
///
/// Methods in this class are often prefixed with "Global" or "Local", depending
/// on the guarantees that they need to satisfy. These guarantee requirements
/// are determined by the users of a particular method.
class Engine {
 public:
  virtual ~Engine() = default;

  /// Begins a transaction and returns a pointer to it's object.
  virtual Transaction *Begin() = 0;

  /// Advances the command on the transaction with the given id.
  virtual CommandId Advance(TransactionId id) = 0;

  /// Updates the command on the workers to the master's value.
  virtual CommandId UpdateCommand(TransactionId id) = 0;

  /// Comits the given transaction. Deletes the transaction object, it's not
  /// valid after this function executes.
  virtual void Commit(const Transaction &t) = 0;

  /// Aborts the given transaction. Deletes the transaction object, it's not
  /// valid after this function executes.
  virtual void Abort(const Transaction &t) = 0;

  /// Returns the commit log Info about the given transaction.
  virtual CommitLog::Info Info(TransactionId tx) const = 0;

  /// Returns the snapshot relevant to garbage collection of database records.
  ///
  /// If there are no active transactions that means a snapshot containing only
  /// the next transaction ID.  If there are active transactions, that means the
  /// oldest active transaction's snapshot, with that transaction's ID appened
  /// as last.
  ///
  /// The idea is that data records can only be deleted if they were expired
  /// (and that was committed) by a transaction older than the older currently
  /// active. We need the full snapshot to prevent overlaps (see general GC
  /// documentation).
  ///
  /// The returned snapshot must be for the globally oldest active transaction.
  /// If we only looked at locally known transactions, it would be possible to
  /// delete something that and older active transaction can still see.
  virtual Snapshot GlobalGcSnapshot() = 0;

  /// Returns active transactions.
  virtual Snapshot GlobalActiveTransactions() = 0;

  /// Returns the ID the last globally known transaction.
  virtual tx::TransactionId GlobalLast() const = 0;

  /// Returns the ID of last locally known transaction.
  virtual tx::TransactionId LocalLast() const = 0;

  /// Returns the ID of the oldest transaction locally known to be active. It is
  /// guaranteed that all the transactions older than the returned are globally
  /// not active.
  virtual TransactionId LocalOldestActive() const = 0;

  /// Calls function f on each locally active transaction.
  virtual void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) = 0;

  /// Gets a transaction object for a running transaction.
  virtual tx::Transaction *RunningTransaction(TransactionId tx_id) = 0;

  /// Ensures the next transaction that starts will have the ID greater than
  /// the given id.
  virtual void EnsureNextIdGreater(TransactionId tx_id) = 0;

  /// Garbage collects transactions older than tx_id from commit log.
  virtual void GarbageCollectCommitLog(TransactionId tx_id) = 0;

  auto &local_lock_graph() { return local_lock_graph_; }
  const auto &local_lock_graph() const { return local_lock_graph_; }

 protected:
  Transaction *CreateTransaction(TransactionId id, const Snapshot &snapshot) {
    return new Transaction(id, snapshot, *this, false);
  }

  CommandId AdvanceCommand(Transaction *t) { return t->AdvanceCommand(); }

  void SetCommand(Transaction *t, CommandId cid) { t->SetCommand(cid); }

 private:
  // Map lock dependencies. Each entry maps (tx_that_wants_lock,
  // tx_that_holds_lock). Used for local deadlock resolution.
  // TODO consider global deadlock resolution.
  ConcurrentMap<TransactionId, TransactionId> local_lock_graph_;
};
}  // namespace tx
