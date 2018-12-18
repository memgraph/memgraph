#include "transactions/single_node_ha/engine.hpp"

#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "durability/single_node_ha/state_delta.hpp"

namespace tx {

Engine::Engine(raft::RaftInterface *raft)
    : clog_(std::make_unique<CommitLog>()), raft_(raft) {
  CHECK(raft) << "Raft can't be nullptr in HA";
}

Transaction *Engine::Begin() {
  VLOG(11) << "[Tx] Starting transaction " << counter_ + 1;
  std::lock_guard<utils::SpinLock> guard(lock_);
  if (!accepting_transactions_.load())
    throw TransactionEngineError(
        "The transaction engine currently isn't accepting new transactions.");

  return BeginTransaction(false);
}

Transaction *Engine::BeginBlocking(
    std::experimental::optional<TransactionId> parent_tx) {
  Snapshot wait_for_txs;
  {
    std::lock_guard<utils::SpinLock> guard(lock_);
    if (!accepting_transactions_.load())
      throw TransactionEngineError("Engine is not accepting new transactions");

    // Block the engine from accepting new transactions.
    accepting_transactions_.store(false);

    // Set active transactions to abort ASAP.
    for (auto transaction : active_) {
      store_.find(transaction)->second->set_should_abort();
    }

    wait_for_txs = active_;
  }

  // Wait for all active transactions except the parent (optional) and ourselves
  // to end.
  for (auto id : wait_for_txs) {
    if (parent_tx && *parent_tx == id) continue;
    while (Info(id).is_active()) {
      // TODO reconsider this constant, currently rule-of-thumb chosen
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  // Only after all transactions have finished, start the blocking transaction.
  std::lock_guard<utils::SpinLock> guard(lock_);
  return BeginTransaction(true);
}

CommandId Engine::Advance(TransactionId id) {
  std::lock_guard<utils::SpinLock> guard(lock_);

  auto it = store_.find(id);
  DCHECK(it != store_.end())
      << "Transaction::advance on non-existing transaction";

  return it->second.get()->AdvanceCommand();
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
  raft_->Emplace(database::StateDelta::TxCommit(t.id_));

  // Wait for Raft to receive confirmation from the majority of followers.
  while (!raft_->SafeToCommit(t.id_)) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  std::lock_guard<utils::SpinLock> guard(lock_);
  clog_->set_committed(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
  if (t.blocking()) {
    accepting_transactions_.store(true);
  }
}

void Engine::Abort(const Transaction &t) {
  VLOG(11) << "[Tx] Aborting transaction " << t.id_;
  std::lock_guard<utils::SpinLock> guard(lock_);
  clog_->set_aborted(t.id_);
  active_.remove(t.id_);
  raft_->Emplace(database::StateDelta::TxAbort(t.id_));
  store_.erase(store_.find(t.id_));
  if (t.blocking()) {
    accepting_transactions_.store(true);
  }
}

CommitLog::Info Engine::Info(TransactionId tx) const {
  return clog_->fetch_info(tx);
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
  clog_->garbage_collect_older(tx_id);
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

void Engine::Reset() {
  Snapshot wait_for_txs;
  {
    std::lock_guard<utils::SpinLock> guard(lock_);

    // Block the engine from accepting new transactions.
    accepting_transactions_.store(false);

    // Set active transactions to abort ASAP.
    for (auto transaction : active_) {
      store_.find(transaction)->second->set_should_abort();
    }

    wait_for_txs = active_;
  }

  // Wait for all active transactions to end.
  for (auto id : wait_for_txs) {
    while (Info(id).is_active()) {
      // TODO reconsider this constant, currently rule-of-thumb chosen
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  // Only after all transactions have finished, reset the engine.
  std::lock_guard<utils::SpinLock> guard(lock_);
  counter_ = 0;
  store_.clear();
  active_.clear();
  {
    clog_ = nullptr;
    clog_ = std::make_unique<CommitLog>();
  }
  // local_lock_graph_ should be empty because all transactions should've finish
  // by now.
  accepting_transactions_.store(true);
}

Transaction *Engine::BeginTransaction(bool blocking) {
  TransactionId id{++counter_};
  Transaction *t = new Transaction(id, active_, *this, blocking);
  active_.insert(id);
  store_.emplace(id, t);
  raft_->Emplace(database::StateDelta::TxBegin(id));
  return t;
}

}  // namespace tx
