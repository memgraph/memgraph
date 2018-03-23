#pragma once

#include <experimental/optional>
#include <unordered_map>

#include "durability/wal.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"
#include "utils/exceptions.hpp"

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
  command_id_t Advance(transaction_id_t id) override;
  command_id_t UpdateCommand(transaction_id_t id) override;
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;
  CommitLog::Info Info(transaction_id_t tx) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  transaction_id_t GlobalLast() const override;
  transaction_id_t LocalLast() const override;
  transaction_id_t LocalOldestActive() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  Transaction *RunningTransaction(transaction_id_t tx_id) override;
  void EnsureNextIdGreater(transaction_id_t tx_id) override;

 private:
  transaction_id_t counter_{0};
  CommitLog clog_;
  std::unordered_map<transaction_id_t, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  mutable SpinLock lock_;
  // Optional. If present, the Engine will write tx Begin/Commit/Abort
  // atomically (while under lock).
  durability::WriteAheadLog *wal_{nullptr};
};
}  // namespace tx
