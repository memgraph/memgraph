#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "transactions/engine_master.hpp"

namespace tx {
Transaction *MasterEngine::Begin() {
  std::lock_guard<SpinLock> guard(lock_);

  transaction_id_t id{++counter_};
  auto t = new Transaction(id, active_, *this);

  active_.insert(id);
  store_.emplace(id, t);

  return t;
}

void MasterEngine::Advance(transaction_id_t id) {
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

void MasterEngine::Commit(const Transaction &t) {
  std::lock_guard<SpinLock> guard(lock_);
  clog_.set_committed(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
}

void MasterEngine::Abort(const Transaction &t) {
  std::lock_guard<SpinLock> guard(lock_);
  clog_.set_aborted(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
}

CommitLog::Info MasterEngine::Info(transaction_id_t tx) const {
  return clog_.fetch_info(tx);
}

Snapshot MasterEngine::GlobalGcSnapshot() {
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

Snapshot MasterEngine::GlobalActiveTransactions() {
  std::lock_guard<SpinLock> guard(lock_);
  Snapshot active_transactions = active_;
  return active_transactions;
}

bool MasterEngine::GlobalIsActive(transaction_id_t tx) const {
  return clog_.is_active(tx);
}

tx::transaction_id_t MasterEngine::LocalLast() const { return counter_.load(); }

void MasterEngine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  std::lock_guard<SpinLock> guard(lock_);
  for (auto transaction : active_) {
    f(*store_.find(transaction)->second);
  }
}
}  // namespace tx
