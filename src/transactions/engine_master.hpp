#pragma once

#include <atomic>
#include <experimental/optional>
#include <unordered_map>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
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

/**
 * A transaction engine that contains everything necessary for transactional
 * handling. Used for single-node Memgraph deployments and for the master in a
 * distributed system.
 */
class MasterEngine : public Engine {
 public:
  /**
   * @param wal - Optional. If present, the Engine will write tx
   * Begin/Commit/Abort atomically (while under lock).
   */
  MasterEngine(durability::WriteAheadLog *wal = nullptr);

  /**
   * Begins a transaction and returns a pointer to
   * it's object.
   *
   * The transaction object is owned by this engine.
   * It will be released when the transaction gets
   * committted or aborted.
   */
  Transaction *Begin();

  /**
   * Advances the command on the transaction with the
   * given id.
   *
   * @param id - Transation id. That transaction must
   * be currently active.
   */
  void Advance(transaction_id_t id);

  /** Comits the given transaction. Deletes the transaction object, it's not
   * valid after this function executes. */
  void Commit(const Transaction &t);

  /** Aborts the given transaction. Deletes the transaction object, it's not
   * valid after this function executes. */
  void Abort(const Transaction &t);

  CommitLog::Info Info(transaction_id_t tx) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  bool GlobalIsActive(transaction_id_t tx) const override;
  tx::transaction_id_t LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;

  /** Starts the RPC server of the master transactional engine. */
  void StartServer(communication::messaging::System &system);

 private:
  std::atomic<transaction_id_t> counter_{0};
  CommitLog clog_;
  std::unordered_map<transaction_id_t, std::unique_ptr<Transaction>> store_;
  Snapshot active_;
  SpinLock lock_;
  // Optional. If present, the Engine will write tx Begin/Commit/Abort
  // atomically (while under lock).
  durability::WriteAheadLog *wal_{nullptr};

  // Optional RPC server, only used in distributed, not in single_node.
  std::experimental::optional<communication::rpc::Server> rpc_server_;
};
}  // namespace tx
