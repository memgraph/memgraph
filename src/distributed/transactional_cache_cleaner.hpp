#pragma once

#include <functional>
#include <vector>

#include "transactions/engine.hpp"
#include "utils/scheduler.hpp"

namespace distributed {

/// Periodically calls `ClearCache(oldest_transaction)` on all registered
/// functions.
class TransactionalCacheCleaner {
  /// The wait time between two releases of local transaction objects that have
  /// expired on the master.
  static constexpr std::chrono::seconds kCacheReleasePeriod{1};

 public:
  TransactionalCacheCleaner(tx::Engine &tx_engine) : tx_engine_(tx_engine) {
    cache_clearing_scheduler_.Run(
        "DistrTxCacheGc", kCacheReleasePeriod, [this]() {
          auto oldest_active = tx_engine_.LocalOldestActive();
          for (auto &f : functions_) f(oldest_active);
        });
  }

  /// Registers the given object for transactional cleaning. The object will
  /// periodically get it's `ClearCache(tx::transaction_id_t)` method called
  /// with the oldest active transaction id. Note that the ONLY guarantee for
  /// the call param is that there are no transactions alive that have an id
  /// lower than it.
  template <typename TCache>
  void Register(TCache &cache) {
    functions_.emplace_back([&cache](tx::transaction_id_t oldest_active) {
      cache.ClearTransactionalCache(oldest_active);
    });
  }

 private:
  tx::Engine &tx_engine_;
  std::vector<std::function<void(tx::transaction_id_t &oldest_active)>>
      functions_;
  Scheduler cache_clearing_scheduler_;
};
}  // namespace distributed
