/// @file
#pragma once

#include "communication/rpc/client_pool.hpp"
#include "distributed/storage_gc_rpc_messages.hpp"
#include "storage/distributed/storage_gc_distributed.hpp"
#include "transactions/distributed/engine_worker.hpp"

namespace database {

/// Storage garbage collector specific for the worker implementation.
/// Starts a periodic garbage collection that will also send information about
/// the cleared local transactions to the master using RPC.
class StorageGcWorker final : public StorageGcDistributed {
 public:
  StorageGcWorker() = delete;
  StorageGcWorker(const StorageGcWorker &) = delete;
  StorageGcWorker(StorageGcWorker &&) = delete;
  StorageGcWorker operator=(const StorageGcWorker &) = delete;
  StorageGcWorker operator=(StorageGcWorker &&) = delete;

  StorageGcWorker(Storage *storage, tx::Engine *tx_engine, int pause_sec,
                  communication::rpc::ClientPool *master_client_pool,
                  int worker_id)
      : StorageGcDistributed(storage, tx_engine, pause_sec),
        master_client_pool_(master_client_pool),
        worker_id_(worker_id) {}

  void CollectCommitLogGarbage(tx::TransactionId oldest_active,
                               tx::Engine *tx_engine) override {
    // We first need to delete transactions that we can delete to be sure that
    // the locks are released as well. Otherwise some new transaction might
    // try to acquire a lock which hasn't been released (if the transaction
    // cache cleaner was not scheduled at this time), and take a look into the
    // commit log which no longer contains that transaction id.
    // TODO: when I (mferencevic) refactored the transaction engine code, I
    // found out that the function `ClearTransactionalCache` of the
    // `tx::EngineWorker` was called periodically in the transactional cache
    // cleaner. That code was then moved and can now be found in the
    // `tx::EngineDistributed` garbage collector. This may not be correct,
    // @storage_team please investigate this.
    dynamic_cast<tx::EngineWorker &>(*tx_engine)
        .ClearTransactionalCache(oldest_active);
    auto safe_to_delete = GetClogSafeTransaction(oldest_active);
    if (safe_to_delete) {
      master_client_pool_->Call<distributed::RanLocalGcRpc>(*safe_to_delete,
                                                            worker_id_);
      tx_engine->GarbageCollectCommitLog(*safe_to_delete);
    }
  }

 private:
  communication::rpc::ClientPool *master_client_pool_;
  int worker_id_;
};
}  // namespace database
