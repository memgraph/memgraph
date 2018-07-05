#pragma once

#include <functional>
#include <vector>

#include "communication/rpc/server.hpp"
#include "distributed/produce_rpc_server.hpp"
#include "distributed/transactional_cache_cleaner_rpc_messages.hpp"
#include "transactions/engine.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/scheduler.hpp"

namespace distributed {

/// Periodically calls `ClearTransactionalCache(oldest_transaction)` on all
/// registered objects.
class TransactionalCacheCleaner {
  /// The wait time between two releases of local transaction objects that have
  /// expired on the master.
  static constexpr std::chrono::seconds kCacheReleasePeriod{1};

 public:
  template <typename... T>
  TransactionalCacheCleaner(tx::Engine &tx_engine, T &... caches)
      : tx_engine_(tx_engine) {
    Register(caches...);
    cache_clearing_scheduler_.Run(
        "DistrTxCacheGc", kCacheReleasePeriod,
        [this]() { this->Clear(tx_engine_.GlobalGcSnapshot().back()); });
  }

 protected:
  /// Registers the given object for transactional cleaning. The object will
  /// periodically get it's `ClearCache(tx::TransactionId)` method called
  /// with the oldest active transaction id. Note that the ONLY guarantee for
  /// the call param is that there are no transactions alive that have an id
  /// lower than it.
  template <typename TCache>
  void Register(TCache &cache) {
    functions_.emplace_back([&cache](tx::TransactionId oldest_active) {
      cache.ClearTransactionalCache(oldest_active);
    });
  }

 private:
  template <typename TCache, typename... T>
  void Register(TCache &cache, T &... caches) {
    Register(cache);
    Register(caches...);
  }

  void Clear(tx::TransactionId oldest_active) {
    for (auto &f : functions_) f(oldest_active);
  }

  tx::Engine &tx_engine_;
  std::vector<std::function<void(tx::TransactionId &oldest_active)>> functions_;
  utils::Scheduler cache_clearing_scheduler_;
};

/// Registers a RPC server that listens for `WaitOnTransactionEnd` requests
/// that require all ongoing produces to finish. It also periodically calls
/// `ClearTransactionalCache` on all registered objects.
class WorkerTransactionalCacheCleaner : public TransactionalCacheCleaner {
 public:
  template <class... T>
  WorkerTransactionalCacheCleaner(tx::WorkerEngine &tx_engine,
                                  durability::WriteAheadLog *wal,
                                  communication::rpc::Server &server,
                                  ProduceRpcServer &produce_server,
                                  T &... caches)
      : TransactionalCacheCleaner(tx_engine, caches...),
        wal_(wal),
        rpc_server_(server),
        produce_server_(produce_server) {
    Register(tx_engine);
    rpc_server_.Register<WaitOnTransactionEndRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          auto tx_id = req_reader.getTxId();
          auto committed = req_reader.getCommitted();
          produce_server_.FinishAndClearOngoingProducePlans(tx_id);
          if (wal_) {
            if (committed) {
              wal_->Emplace(database::StateDelta::TxCommit(tx_id));
            } else {
              wal_->Emplace(database::StateDelta::TxAbort(tx_id));
            }
          }
        });
  }

 private:
  durability::WriteAheadLog *wal_;
  communication::rpc::Server &rpc_server_;
  ProduceRpcServer &produce_server_;
};

}  // namespace distributed
