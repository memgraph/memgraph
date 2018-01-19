#pragma once

#include <atomic>
#include <mutex>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "io/network/endpoint.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

namespace tx {

/** Distributed worker transaction engine. Connects to a MasterEngine (single
 * source of truth) to obtain transactional info. Caches most info locally. */
class WorkerEngine : public Engine {
 public:
  WorkerEngine(communication::messaging::System &system,
               const io::network::Endpoint &endpoint);

  CommitLog::Info Info(transaction_id_t tid) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  bool GlobalIsActive(transaction_id_t tid) const override;
  tx::transaction_id_t LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;
  tx::Transaction *RunningTransaction(tx::transaction_id_t tx_id) override;

 private:
  // Local caches.
  ConcurrentMap<transaction_id_t, Transaction *> active_;
  std::atomic<transaction_id_t> local_last_{0};
  // Mutable because just getting info can cause a cache fill.
  mutable CommitLog clog_;

  // Communication to the transactional master.
  mutable communication::rpc::Client rpc_client_;
};

}  // namespace tx
