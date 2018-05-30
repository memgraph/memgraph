#pragma once

#include <atomic>
#include <experimental/optional>
#include <unordered_map>

#include "durability/wal.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"
#include "utils/exceptions.hpp"
#include "utils/thread/sync.hpp"

namespace tx {

/** Indicates an error in transaction handling (currently
 * only command id overflow). */
class TransactionError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/** Single-node deployment transaction engine. Has complete functionality. */
class SingleNodeEngine : public Engine {
 public:
  /**
   * @param wal - Optional. If present, the Engine will write tx
   * Begin/Commit/Abort atomically (while under lock).
   */
  explicit SingleNodeEngine(durability::WriteAheadLog *wal = nullptr);

  Transaction *Begin() override;
  CommandId Advance(TransactionId id) override;
  CommandId UpdateCommand(TransactionId id) override;
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;
  CommitLog::Info Info(TransactionId tx) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  TransactionId GlobalLast() const override;
  TransactionId LocalLast() const override;
  TransactionId LocalOldestActive() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  Transaction *RunningTransaction(TransactionId tx_id) override;
  void EnsureNextIdGreater(TransactionId tx_id) override;
  void GarbageCollectCommitLog(TransactionId tx_id) override;

 private:
  TransactionId counter_{0};
  CommitLog clog_;
  std::unordered_map<TransactionId, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  mutable utils::SpinLock lock_;
  // Optional. If present, the Engine will write tx Begin/Commit/Abort
  // atomically (while under lock).
  durability::WriteAheadLog *wal_{nullptr};
};
}  // namespace tx
