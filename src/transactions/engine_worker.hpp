#pragma once

#include <atomic>
#include <mutex>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

namespace tx {
/** A transactional engine for the worker in a distributed system. */
class WorkerEngine : public Engine {
 public:
  WorkerEngine(communication::messaging::System &system,
               const std::string &tx_server_host, uint16_t tx_server_port);

  Transaction *LocalBegin(transaction_id_t tx_id);

  CommitLog::Info Info(transaction_id_t tid) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  bool GlobalIsActive(transaction_id_t tid) const override;
  tx::transaction_id_t LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;

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
