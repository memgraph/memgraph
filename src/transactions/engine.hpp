#pragma once

#include <atomic>
#include <vector>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "transactions/transaction_store.hpp"
#include "utils/counters/simple_counter.hpp"

namespace tx {

class TransactionError : std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class Engine : Lockable<SpinLock> {
 public:
  using sptr = std::shared_ptr<Engine>;

  Engine() : counter(1) {}

  // Begins transaction and runs given functions in same atomic step.
  // Functions will be given Transaction&
  template <class... F>
  Transaction *begin(F... fun) {
    auto guard = this->acquire_unique();

    auto id = Id(counter.next());
    auto t = new Transaction(id, active, *this);

    active.insert(id);
    store.put(id, t);

    call(*t, fun...);

    return t;
  }

  Transaction &advance(const Id &id) {
    auto guard = this->acquire_unique();

    auto *t = store.get(id);

    if (t == nullptr) throw TransactionError("transaction does not exist");

    // this is a new command
    if (t->cid == 255)
      throw TransactionError(
          "Reached maximum number of commands in this transaction.");
    t->cid++;

    return *t;
  }

  // Returns copy of current snapshot
  Snapshot<Id> snapshot() {
    auto guard = this->acquire_unique();

    return active;
  }

  void commit(const Transaction &t) {
    auto guard = this->acquire_unique();
    clog.set_committed(t.id);

    finalize(t);
  }

  void abort(const Transaction &t) {
    auto guard = this->acquire_unique();
    clog.set_aborted(t.id);

    finalize(t);
  }

  Id last_known_active() {
    auto guard = this->acquire_unique();
    return active.front();
  }

  // total number of transactions started from the beginning of time
  uint64_t count() {
    auto guard = this->acquire_unique();
    return counter.count();
  }

  // the number of currently active transactions
  size_t size() {
    auto guard = this->acquire_unique();
    return active.size();
  }

  CommitLog clog;

 private:
  template <class T, class... F>
  void call(Transaction &t, T fun, F... funs) {
    call(t, fun);
    call(t, funs...);
  }

  template <class T>
  void call(Transaction &t, T fun) {
    fun(t);
  }

  void call(Transaction &t) {}

  void finalize(const Transaction &t) {
    active.remove(t.id);

    // remove transaction from store
    store.del(t.id);
  }

  SimpleCounter<uint64_t> counter;
  Snapshot<Id> active;
  TransactionStore<uint64_t> store;
};
}
