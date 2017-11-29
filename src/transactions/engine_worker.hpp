#pragma once

#include <atomic>
#include <memory>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

namespace tx {
/** A transactional engine for the worker in a distributed system. */
class WorkerEngine : public Engine {
  // Mock class for RPC.
  // TODO Replace with the real thing, once available.
  class Rpc {
   public:
    template <typename TReturn, typename... TArgs>
    TReturn Call(const std::string &, TArgs &&...) {
      return TReturn{};
    }

    template <typename TReturn, typename... TArgs>
    TReturn Call(const std::string &, TReturn default_return, TArgs &&...) {
      return default_return;
    }
  };

 public:
  WorkerEngine(Rpc &rpc) : rpc_(rpc) {}

  Transaction *LocalBegin(transaction_id_t tx_id);

  CommitLog::Info Info(transaction_id_t tid) const override;
  Snapshot GlobalGcSnapshot() override;
  Snapshot GlobalActiveTransactions() override;
  bool GlobalIsActive(transaction_id_t tid) const override;
  tx::transaction_id_t LocalLast() const override;
  void LocalForEachActiveTransaction(
      std::function<void(Transaction &)> f) override;

 private:
  // Communication with the transactional engine on the master.
  Rpc &rpc_;

  // Local caches.
  ConcurrentMap<transaction_id_t, Transaction *> active_;
  std::atomic<transaction_id_t> local_last_;
  // Mutable because just getting info can cause a cache fill.
  mutable CommitLog clog_;
};
}  // namespace tx
