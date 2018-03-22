#pragma once

#include <algorithm>
#include <mutex>
#include <vector>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "transactions/type.hpp"

namespace tx {
/**
 * Database transaction engine. Used for managing transactions and the related
 * information such as transaction snapshots and the transaction state info.
 *
 * This is an abstract base class for implementing a single-node transactional
 * engine (MasterEngine), an engine for the master in a distributed system (also
 * MasterEngine), and for the worker in a distributed system (WorkerEngine).
 *
 * Methods in this class are often prefixed with "Global" or "Local", depending
 * on the guarantees that they need to satisfy. These guarantee requirements are
 * determined by the users of a particular method.
 */
class Engine {
 public:
  virtual ~Engine() = default;

  /// Begins a transaction and returns a pointer to it's object.
  virtual Transaction *Begin() = 0;

  /// Advances the command on the transaction with the given id.
  virtual command_id_t Advance(transaction_id_t id) = 0;

  /// Updates the command on the workers to the master's value.
  virtual command_id_t UpdateCommand(transaction_id_t id) = 0;

  /// Comits the given transaction. Deletes the transaction object, it's not
  /// valid after this function executes.
  virtual void Commit(const Transaction &t) = 0;

  /// Aborts the given transaction. Deletes the transaction object, it's not
  /// valid after this function executes.
  virtual void Abort(const Transaction &t) = 0;

  /** Returns the commit log Info about the given transaction. */
  virtual CommitLog::Info Info(transaction_id_t tx) const = 0;

  /** Returns the snapshot relevant to garbage collection of database records.
   *
   * If there are no active transactions that means a snapshot containing only
   * the next transaction ID.  If there are active transactions, that means the
   * oldest active transaction's snapshot, with that transaction's ID appened as
   * last.
   *
   * The idea is that data records can only be deleted if they were expired (and
   * that was committed) by a transaction older than the older currently active.
   * We need the full snapshot to prevent overlaps (see general GC
   * documentation).
   *
   * The returned snapshot must be for the globally oldest active transaction.
   * If we only looked at locally known transactions, it would be possible to
   * delete something that and older active transaction can still see.
   */
  virtual Snapshot GlobalGcSnapshot() = 0;

  /** Returns active transactions. */
  virtual Snapshot GlobalActiveTransactions() = 0;

  /** Returns the ID of last locally known transaction. */
  virtual tx::transaction_id_t LocalLast() const = 0;

  /** Returns the ID of the oldest transaction locally known to be active. It is
   * guaranteed that all the transactions older than the returned are globally
   * not active. */
  virtual transaction_id_t LocalOldestActive() const = 0;

  /** Calls function f on each locally active transaction. */
  virtual void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) = 0;

  /** Gets a transaction object for a running transaction. */
  virtual tx::Transaction *RunningTransaction(transaction_id_t tx_id) = 0;

  /** Ensures the next transaction that starts will have the ID greater than
   * the given id. */
  virtual void EnsureNextIdGreater(transaction_id_t tx_id) = 0;

  auto &local_lock_graph() { return local_lock_graph_; }
  const auto &local_lock_graph() const { return local_lock_graph_; }

 private:
  // Map lock dependencies. Each entry maps (tx_that_wants_lock,
  // tx_that_holds_lock). Used for local deadlock resolution.
  // TODO consider global deadlock resolution.
  ConcurrentMap<transaction_id_t, transaction_id_t> local_lock_graph_;
};
}  // namespace tx
