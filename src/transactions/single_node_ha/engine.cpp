#include "transactions/single_node_ha/engine.hpp"

#include <limits>
#include <mutex>

#include "glog/logging.h"

#include "durability/single_node_ha/state_delta.hpp"
#include "raft/exceptions.hpp"

namespace tx {

Engine::Engine(raft::RaftInterface *raft)
    : clog_(std::make_unique<CommitLog>()), raft_(raft) {
  CHECK(raft) << "Raft can't be nullptr in HA";
}

Transaction *Engine::Begin() {
  VLOG(11) << "[Tx] Starting transaction " << counter_ + 1;
  std::lock_guard<utils::SpinLock> guard(lock_);
  if (!accepting_transactions_.load() || !replication_errors_.empty())
    throw TransactionEngineError(
        "The transaction engine currently isn't accepting new transactions.");

  return BeginTransaction(false);
}

Transaction *Engine::BeginBlocking(std::optional<TransactionId> parent_tx) {
  Snapshot wait_for_txs;
  {
    std::lock_guard<utils::SpinLock> guard(lock_);
    if (!accepting_transactions_.load() || !replication_errors_.empty())
      throw TransactionEngineError(
          "The transaction engine currently isn't accepting new transactions.");

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
  VLOG(11) << "[Tx] Committing transaction " << t.id_;
  auto delta_status = raft_->Emplace(database::StateDelta::TxCommit(t.id_));
  if (delta_status.emplaced) {
    // It is important to note the following situation.  If our cluster ends up
    // with a network partition where the current leader can't communicate with
    // the majority of the peers, and the client is still sending queries to it,
    // all of the transaction will end up waiting here until the network
    // partition is resolved.  The problem that can occur afterwards is bad.
    // When the machine transitions from leader to follower mode,
    // `ReplicationInfo` method will start returning `is_replicated=true`. This
    // might lead to a problem where we suddenly want to alter the state of the
    // transaction engine that isn't valid anymore, because the current machine
    // isn't the leader anymore. This is all handled in the `Transition` method
    // where once the transition from leader to follower  occurs, the mode will
    // be set to follower first, then the `Reset` method on the transaction
    // engine will wait for all transactions to finish, and even though we
    // change the transaction engine state here, the engine will perform a
    // `Reset` and start recovering from zero, and the invalid changes won't
    // matter.

    // Wait for Raft to receive confirmation from the majority of followers.
    while (true) {
      try {
        if (raft_->SafeToCommit(t.id_)) break;
      } catch (const raft::ReplicationTimeoutException &e) {
        std::lock_guard<utils::SpinLock> guard(lock_);
        if (replication_errors_.insert(t.id_).second) {
          LOG(WARNING) << e.what();
        }
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    std::unique_lock<std::mutex> raft_lock(raft_->WithLock(), std::defer_lock);
    // We need to acquire the Raft lock so we don't end up racing with a Raft
    // thread that can reset the engine state. If we can't acquire the lock, and
    // we end up with reseting the engine, we throw
    // UnexpectedLeaderChangeException.
    while (true) {
      if (raft_lock.try_lock()) {
        break;
      }
      // This is the case when we've lost our leader status due to another peer
      // requesting election.
      if (reset_active_.load()) throw raft::UnexpectedLeaderChangeException();
      // This is the case when we're shutting down and we're no longer a valid
      // leader. `SafeToCommit` will throw `RaftShutdownException` if the
      // transaction wasn't replicated and the client will receive a negative
      // response. Otherwise, we'll end up here, and since the transaction was
      // replciated, we need to inform the client that the query succeeded.
      if (!raft_->IsLeader()) break;
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  std::lock_guard<utils::SpinLock> guard(lock_);
  replication_errors_.erase(t.id_);
  clog_->set_committed(t.id_);
  active_.remove(t.id_);
  store_.erase(store_.find(t.id_));
  if (t.blocking()) {
    accepting_transactions_.store(true);
  }
}

void Engine::Abort(const Transaction &t) {
  VLOG(11) << "[Tx] Aborting transaction " << t.id_;
  raft_->Emplace(database::StateDelta::TxAbort(t.id_));
  std::lock_guard<utils::SpinLock> guard(lock_);
  clog_->set_aborted(t.id_);
  active_.remove(t.id_);
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
    reset_active_.store(true);
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
  replication_errors_.clear();
  store_.clear();
  active_.clear();
  {
    clog_ = nullptr;
    clog_ = std::make_unique<CommitLog>();
  }
  accepting_transactions_.store(true);
  reset_active_.store(false);
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
