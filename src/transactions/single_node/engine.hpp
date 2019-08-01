/// @file

#pragma once

#include <atomic>
#include <optional>
#include <unordered_map>

#include "durability/single_node/wal.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/transaction.hpp"
#include "utils/spin_lock.hpp"

namespace tx {

class TransactionEngineError : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Single-node deployment transaction engine. Has complete functionality.
class Engine final {
 public:
  /// @param wal - Optional. If present, the Engine will write tx
  /// Begin/Commit/Abort atomically (while under lock).
  explicit Engine(durability::WriteAheadLog *wal = nullptr);

  Engine(const Engine &) = delete;
  Engine(Engine &&) = delete;
  Engine &operator=(const Engine &) = delete;
  Engine &operator=(Engine &&) = delete;

  /// @throw TransactionEngineError
  Transaction *Begin();
  /// Blocking transactions are used when we can't allow any other transaction to
  /// run (besides this one). This is the reason why this transactions blocks the
  /// engine from creating new transactions and waits for the existing ones to
  /// finish.
  /// @throw TransactionEngineError
  Transaction *BeginBlocking(std::optional<TransactionId> parent_tx);
  /// @throw TransactionException
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

 private:
  // Map lock dependencies. Each entry maps (tx_that_wants_lock,
  // tx_that_holds_lock). Used for local deadlock resolution.
  // TODO consider global deadlock resolution.
  ConcurrentMap<TransactionId, TransactionId> local_lock_graph_;

  TransactionId counter_{0};
  CommitLog clog_;
  std::unordered_map<TransactionId, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  mutable utils::SpinLock lock_;
  // Optional. If present, the Engine will write tx Begin/Commit/Abort
  // atomically (while under lock).
  durability::WriteAheadLog *wal_{nullptr};
  std::atomic<bool> accepting_transactions_{true};

  // Helper method for transaction begin.
  Transaction *BeginTransaction(bool blocking);
};
}  // namespace tx
