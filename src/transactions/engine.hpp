#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "threading/sync/spinlock.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "utils/exceptions.hpp"

namespace tx {

/** Indicates an error in transaction handling (currently
 * only command id overflow). */
class TransactionError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/** Database transaction egine.
 *
 * Used for managing transactions and the related information
 * such as transaction snapshots and the commit log.
 */
class Engine {
 public:
  Engine() = default;

  /** Begins a transaction and returns a pointer to
   * it's object.
   *
   * The transaction object is owned by this engine.
   * It will be released when the transaction gets
   * committted or aborted.
   */
  Transaction *Begin() {
    std::lock_guard<SpinLock> guard(lock_);

    transaction_id_t id{++counter_};
    auto t = new Transaction(id, active_, *this);

    active_.insert(id);
    store_.emplace(id, t);

    return t;
  }

  /** Advances the command on the transaction with the
   * given id.
   *
   * @param id - Transation id. That transaction must
   * be currently active.
   */
  void Advance(transaction_id_t id) {
    std::lock_guard<SpinLock> guard(lock_);

    auto it = store_.find(id);
    DCHECK(it != store_.end())
        << "Transaction::advance on non-existing transaction";

    Transaction *t = it->second.get();
    if (t->cid_ == std::numeric_limits<command_id_t>::max())
      throw TransactionError(
          "Reached maximum number of commands in this "
          "transaction.");

    t->cid_++;
  }

  /** Returns the snapshot relevant to garbage collection of database records.
   *
   * If there are no active transactions that means a snapshot containing only
   * the next transaction ID.  If there are active transactions, that means the
   * oldest active transaction's snapshot, with that transaction's ID appened as
   * last.
   *
   * The idea is that data records can only be deleted if they were expired (and
   * that was committed) by a transaction older then the older currently active.
   * We need the full snapshot to prevent overlaps (see general GC
   * documentation).
   */
  Snapshot GcSnapshot() {
    std::lock_guard<SpinLock> guard(lock_);

    // No active transactions.
    if (active_.size() == 0) {
      auto snapshot_copy = active_;
      snapshot_copy.insert(counter_ + 1);
      return snapshot_copy;
    }

    // There are active transactions.
    auto snapshot_copy = store_.find(active_.front())->second->snapshot();
    snapshot_copy.insert(active_.front());
    return snapshot_copy;
  }

  /**
   * Returns active transactions.
   */
  Snapshot ActiveTransactions() {
    std::lock_guard<SpinLock> guard(lock_);
    Snapshot active_transactions = active_;
    return active_transactions;
  }

  /** Comits the given transaction. Deletes the transaction object, it's not
   * valid after this function executes. */
  void Commit(const Transaction &t) {
    std::lock_guard<SpinLock> guard(lock_);
    clog_.set_committed(t.id_);
    active_.remove(t.id_);
    store_.erase(store_.find(t.id_));
  }

  /** Aborts the given transaction. Deletes the transaction object, it's not
   * valid after this function executes. */
  void Abort(const Transaction &t) {
    std::lock_guard<SpinLock> guard(lock_);
    clog_.set_aborted(t.id_);
    active_.remove(t.id_);
    store_.erase(store_.find(t.id_));
  }

  /** Returns transaction id of last transaction without taking a lock. New
   * transactions can be created or destroyed during call of this function.
   */
  auto LockFreeCount() const { return counter_.load(); }

  /** Returns true if the transaction with the given ID is currently active. */
  bool IsActive(transaction_id_t tx) const { return clog_.is_active(tx); }

  /** Calls function f on each active transaction. */
  void ForEachActiveTransaction(std::function<void(Transaction &)> f) {
    std::lock_guard<SpinLock> guard(lock_);
    for (auto transaction : active_) {
      f(*store_.find(transaction)->second);
    }
  }

  /** Returns the commit log Info about the given transaction. */
  auto FetchInfo(transaction_id_t tx) const { return clog_.fetch_info(tx); }

  auto &lock_graph() { return lock_graph_; }
  const auto &lock_graph() const { return lock_graph_; }

 private:
  std::atomic<transaction_id_t> counter_{0};
  CommitLog clog_;
  std::unordered_map<transaction_id_t, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  SpinLock lock_;

  // For each active transaction we store a transaction that holds a lock that
  // mentioned transaction is also trying to acquire. We can think of this
  // data structure as a graph: key being a start node of directed edges and
  // value being an end node of that edge. ConcurrentMap is used since it is
  // garbage collected and we are sure that we will not be having problems with
  // lifetimes of each object.
  ConcurrentMap<transaction_id_t, transaction_id_t> lock_graph_;
};
}  // namespace tx
