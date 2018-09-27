#pragma once

#include <atomic>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "durability/wal.hpp"
#include "io/network/endpoint.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/distributed/engine_distributed.hpp"
#include "transactions/transaction.hpp"

namespace tx {

/** Distributed worker transaction engine. Connects to a EngineMaster (single
 * source of truth) to obtain transactional info. Caches most info locally. Can
 * begin/advance/end transactions on the master. */
class EngineWorker final : public EngineDistributed {
 public:
  EngineWorker(communication::rpc::Server *server,
               communication::rpc::ClientPool *master_client_pool,
               durability::WriteAheadLog *wal = nullptr);
  ~EngineWorker();

  EngineWorker(const EngineWorker &) = delete;
  EngineWorker(EngineWorker &&) = delete;
  EngineWorker &operator=(const EngineWorker &) = delete;
  EngineWorker &operator=(EngineWorker &&) = delete;

  Transaction *Begin() override;
  CommandId Advance(TransactionId id) override;
  CommandId UpdateCommand(TransactionId id) override;
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;
  CommitLog::Info Info(TransactionId tid) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  TransactionId GlobalLast() const override;
  TransactionId LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  TransactionId LocalOldestActive() const override;
  Transaction *RunningTransaction(TransactionId tx_id) override;

  // Caches the transaction for the given info an returs a ptr to it.
  Transaction *RunningTransaction(TransactionId tx_id,
                                  const Snapshot &snapshot);

  void EnsureNextIdGreater(TransactionId tx_id) override;
  void GarbageCollectCommitLog(tx::TransactionId tx_id) override;

  void ClearTransactionalCache(TransactionId oldest_active) override;

 private:
  // Local caches.
  mutable ConcurrentMap<TransactionId, Transaction *> active_;
  std::atomic<TransactionId> local_last_{0};
  // Mutable because just getting info can cause a cache fill.
  mutable CommitLog clog_;

  // Our local RPC server.
  communication::rpc::Server *server_;

  // Communication to the transactional master.
  communication::rpc::ClientPool *master_client_pool_;

  // Write ahead log.
  durability::WriteAheadLog *wal_;

  // Used for clearing of caches of transactions that have expired.
  // Initialize the oldest_active_ with 1 because there's never a tx with id=0
  std::atomic<TransactionId> oldest_active_{1};

  // Removes a single transaction from the cache, if present.
  void ClearSingleTransaction(TransactionId tx_Id) const;

  // Updates the oldest active transaction to the one from the snapshot. If the
  // snapshot is empty, it's set to the given alternative.
  void UpdateOldestActive(const Snapshot &snapshot, TransactionId alternative);
};
}  // namespace tx
