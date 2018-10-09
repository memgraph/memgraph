#include "transactions/single_node/engine.hpp"

#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "durability/single_node/state_delta.hpp"

namespace tx {

Engine::Engine(durability::WriteAheadLog *wal) : wal_(wal) {}

Transaction *Engine::Begin() {
  VLOG(11) << "[Tx] Starting transaction " << counter_ + 1;
  std::lock_guard<utils::SpinLock> guard(lock_);

  TransactionId id{++counter_};
  auto t = CreateTransaction(id, active_);
  active_.insert(id);
  store_.emplace(id, t);
  if (wal_) {
    wal_->Emplace(database::StateDelta::TxBegin(id));
  }
  return t;
}

CommandId Engine::Advance(TransactionId id) {
  std::lock_guard<utils::SpinLock> guard(lock_);

  auto it = store_.find(id);
  DCHECK(it != store_.end())
      << "Transaction::advance on non-existing transaction";

  Transaction *t = it->second.get();
  return AdvanceCommand(t);
}

CommandId Engine::UpdateCommand(TransactionId id) {
  std::lock_guard<utils::SpinLock> guard(lock_);
  auto it = store_.find(id);
  DCHECK(it != store_.end())
      << "Transaction::advance on non-existing transaction";
  return it->second->cid();
}

void Engine::Commit(const Transaction &t) {
  VLOG(11) << "[Tx] Commiting transaction " << t.id_;
  std::lock_guard<utils::SpinLock> guard(lock_);
  clog_.set_committed(t.id_);
  active_.remove(t.id_);
  if (wal_) {
    wal_->Emplace(database::StateDelta::TxCommit(t.id_));
  }
  store_.erase(store_.find(t.id_));
}

void Engine::Abort(const Transaction &t) {
  VLOG(11) << "[Tx] Aborting transaction " << t.id_;
  std::lock_guard<utils::SpinLock> guard(lock_);
  clog_.set_aborted(t.id_);
  active_.remove(t.id_);
  if (wal_) {
    wal_->Emplace(database::StateDelta::TxAbort(t.id_));
  }
  store_.erase(store_.find(t.id_));
}

CommitLog::Info Engine::Info(TransactionId tx) const {
  return clog_.fetch_info(tx);
}

Snapshot Engine::GlobalGcSnapshot() {
  std::lock_guard<utils::SpinLock> guard(lock_);

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

Snapshot Engine::GlobalActiveTransactions() {
  std::lock_guard<utils::SpinLock> guard(lock_);
  Snapshot active_transactions = active_;
  return active_transactions;
}

TransactionId Engine::LocalLast() const {
  std::lock_guard<utils::SpinLock> guard(lock_);
  return counter_;
}

TransactionId Engine::GlobalLast() const { return LocalLast(); }

TransactionId Engine::LocalOldestActive() const {
  std::lock_guard<utils::SpinLock> guard(lock_);
  return active_.empty() ? counter_ + 1 : active_.front();
}

void Engine::GarbageCollectCommitLog(TransactionId tx_id) {
  clog_.garbage_collect_older(tx_id);
}

void Engine::LocalForEachActiveTransaction(
    std::function<void(Transaction &)> f) {
  std::lock_guard<utils::SpinLock> guard(lock_);
  for (auto transaction : active_) {
    f(*store_.find(transaction)->second);
  }
}

Transaction *Engine::RunningTransaction(TransactionId tx_id) {
  std::lock_guard<utils::SpinLock> guard(lock_);
  auto found = store_.find(tx_id);
  CHECK(found != store_.end())
      << "Can't return snapshot for an inactive transaction";
  return found->second.get();
}

void Engine::EnsureNextIdGreater(TransactionId tx_id) {
  std::lock_guard<utils::SpinLock> guard(lock_);
  counter_ = std::max(tx_id, counter_);
}

}  // namespace tx
