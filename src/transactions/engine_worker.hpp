#pragma once

#include <atomic>
#include <chrono>
#include <mutex>

#include "communication/rpc/client_pool.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "io/network/endpoint.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"
#include "utils/scheduler.hpp"

namespace tx {

/** Distributed worker transaction engine. Connects to a MasterEngine (single
 * source of truth) to obtain transactional info. Caches most info locally. Can
 * begin/advance/end transactions on the master. */
class WorkerEngine : public Engine {
 public:
  /// The wait time between two releases of local transaction objects that have
  /// expired on the master.
  static constexpr std::chrono::seconds kCacheReleasePeriod{1};

  WorkerEngine(const io::network::Endpoint &endpoint);
  ~WorkerEngine();

  Transaction *Begin() override;
  command_id_t Advance(transaction_id_t id) override;
  command_id_t UpdateCommand(transaction_id_t id) override;
  void Commit(const Transaction &t) override;
  void Abort(const Transaction &t) override;
  CommitLog::Info Info(transaction_id_t tid) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  bool GlobalIsActive(transaction_id_t tid) const override;
  transaction_id_t LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  Transaction *RunningTransaction(transaction_id_t tx_id) override;

  // Caches the transaction for the given info an returs a ptr to it.
  Transaction *RunningTransaction(transaction_id_t tx_id,
                                  const Snapshot &snapshot);

 private:
  // Local caches.
  mutable ConcurrentMap<transaction_id_t, Transaction *> active_;
  std::atomic<transaction_id_t> local_last_{0};
  // Mutable because just getting info can cause a cache fill.
  mutable CommitLog clog_;

  // Communication to the transactional master.
  mutable communication::rpc::ClientPool rpc_client_pool_;

  // Removes (destructs) a Transaction that's expired. If there is no cached
  // transacton for the given id, nothing is done.
  void ClearCache(transaction_id_t tx_id) const;

  // Used for clearing of caches of transactions that have expired.
  // Initialize the oldest_active_ with 1 because there's never a tx with id=0
  std::atomic<transaction_id_t> oldest_active_{1};
  void ClearCachesBasedOnOldest(transaction_id_t oldest_active);
  Scheduler cache_clearing_scheduler_;
};
}  // namespace tx
