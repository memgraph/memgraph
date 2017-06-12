#pragma once

#include <atomic>
#include <limits>
#include <vector>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "transactions/transaction_store.hpp"
#include "utils/counters/simple_counter.hpp"
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
class Engine : Lockable<SpinLock> {
  // limit for the command id, used for checking if we're about
  // to overflow. slightly unneccessary since command id should
  // be a 64-bit int
  static constexpr auto kMaxCommandId =
      std::numeric_limits<decltype(std::declval<Transaction>().cid())>::max();

 public:
  /** Begins a transaction and returns a pointer to
   * it's object.
   *
   * The transaction object is owned by this engine.
   * It will be released when the transaction gets
   * committted or aborted.
   */
  Transaction *Begin() {
    auto guard = this->acquire_unique();

    transaction_id_t id{counter_.next()};
    auto t = new Transaction(id, active_, *this);

    active_.insert(id);
    store_.put(id, t);

    return t;
  }

  /** Advances the command on the transaction with the
   * given id.
   *
   * @param id - Transation id. That transaction must
   * be currently active.
   * @return Pointer to the transaction object for id.
   */
  Transaction &Advance(transaction_id_t id) {
    auto guard = this->acquire_unique();

    auto *t = store_.get(id);
    debug_assert(t != nullptr,
                 "Transaction::advance on non-existing transaction");

    if (t->cid_ == kMaxCommandId)
      throw TransactionError(
          "Reached maximum number of commands in this transaction.");

    t->cid_++;
    return *t;
  }

  /** Returns the snapshot relevant to garbage collection
   * of database records.
   *
   * If there are no active transactions that means
   * a snapshot containing only the next transaction ID.
   * If there are active transactions, that means the
   * oldest active transaction's snapshot, with that
   * transaction's ID appened as last.
   *
   * The idea is that data records can only be deleted
   * if they were expired (and that was committed) by
   * a transaction older then the older currently active.
   * We need the full snapshot to prevent overlaps (see
   * general GC documentation).
   */
  Snapshot GcSnapshot() {
    auto guard = this->acquire_unique();

    // no active transactions
    if (active_.size() == 0) {
      auto snapshot_copy = active_;
      snapshot_copy.insert(counter_.count() + 1);
      return snapshot_copy;
    }

    // there are active transactions
    auto snapshot_copy = store_.get(active_.front())->snapshot();
    snapshot_copy.insert(active_.front());
    return snapshot_copy;
  }

  /** Comits the given transaction. Deletes the transaction
   * object, it's not valid after this function executes. */
  void Commit(const Transaction &t) {
    auto guard = this->acquire_unique();
    clog_.set_committed(t.id_);

    Finalize(t);
  }

  /** Aborts the given transaction. Deletes the transaction
   * object, it's not valid after this function executes. */
  void Abort(const Transaction &t) {
    auto guard = this->acquire_unique();
    clog_.set_aborted(t.id_);

    Finalize(t);
  }

  /** The total number of transactions that have
   * executed since the creation of this engine */
  auto Count() {
    auto guard = this->acquire_unique();
    return counter_.count();
  }

  /** The count of currently active transactions */
  size_t ActiveCount() {
    auto guard = this->acquire_unique();
    return active_.size();
  }

  // TODO make this private and expose "const CommitLog"
  // through a getter. To do that you need to make the
  // appropriate CommitLog functions const. To do THAT,
  // you need to make appropriate DynamicBitset functions
  // const. While doing that, clean the DynamicBitset up.
  /** Commit log of this engine */
  CommitLog clog_;

 private:
  // Performs cleanup common to ending the transaction
  // with either commit or abort
  void Finalize(const Transaction &t) {
    active_.remove(t.id_);
    store_.del(t.id_);
  }

  // transaction counter. contains the number of transactions
  // ever created till now
  SimpleCounter<transaction_id_t> counter_{0};

  // a snapshot of currently active transactions
  Snapshot active_;

  // storage for the transactions
  TransactionStore<transaction_id_t> store_;
};
}
