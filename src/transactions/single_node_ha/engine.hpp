/// @file

#pragma once

#include <atomic>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "raft/raft_interface.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "utils/spin_lock.hpp"

namespace tx {

class TransactionEngineError : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// High availability single node transaction engine.
///
/// Requires RaftInterface where it stores StateDeltas containing transaction
/// information needed for raft followers when replicating logs.
class Engine final {
 public:
  explicit Engine(raft::RaftInterface *raft);

  Engine(const Engine &) = delete;
  Engine(Engine &&) = delete;
  Engine &operator=(const Engine &) = delete;
  Engine &operator=(Engine &&) = delete;

  Transaction *Begin();
  /// Blocking transactions are used when we can't allow any other transaction
  /// to run (besides this one). This is the reason why this transactions blocks
  /// the engine from creating new transactions and waits for the existing ones
  /// to finish.
  Transaction *BeginBlocking(std::optional<TransactionId> parent_tx);
  CommandId Advance(TransactionId id);
  CommandId UpdateCommand(TransactionId id);
  void Commit(const Transaction &t);
  void Abort(const Transaction &t);
  CommitLog::Info Info(TransactionId tx) const;
  Snapshot GlobalGcSnapshot();
  Snapshot GlobalActiveTransactions();
  TransactionId GlobalLast() const;
  TransactionId LocalLast() const;
  TransactionId LocalOldestActive() const;
  void LocalForEachActiveTransaction(std::function<void(Transaction &)> f);
  Transaction *RunningTransaction(TransactionId tx_id);
  void EnsureNextIdGreater(TransactionId tx_id);
  void GarbageCollectCommitLog(TransactionId tx_id);

  auto &local_lock_graph() { return local_lock_graph_; }
  const auto &local_lock_graph() const { return local_lock_graph_; }

  /// Reset the internal state of the engine. Use with caution as this will
  /// block the engine from receiving any new transaction and will hint all
  /// transactions to abort and will wait for them to finish before reseting
  /// engines internal state.
  void Reset();

 private:
  // Map lock dependencies. Each entry maps (tx_that_wants_lock,
  // tx_that_holds_lock). Used for local deadlock resolution.
  // TODO consider global deadlock resolution.
  ConcurrentMap<TransactionId, TransactionId> local_lock_graph_;

  TransactionId counter_{0};
  std::unique_ptr<CommitLog> clog_{nullptr};
  std::unordered_map<TransactionId, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  mutable utils::SpinLock lock_;
  raft::RaftInterface *raft_{nullptr};
  std::atomic<bool> accepting_transactions_{true};

  // Keep track of transaction that experienced a replication error.
  // While there is a replication error known to the engine, the engine won't
  // accept new transactions.
  std::unordered_set<TransactionId> replication_errors_;

  // Helper method for transaction begin.
  Transaction *BeginTransaction(bool blocking);
};
}  // namespace tx
