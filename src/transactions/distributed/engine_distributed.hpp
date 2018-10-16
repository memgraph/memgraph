/// @file

#pragma once

#include "distributed/coordination.hpp"
#include "transactions/distributed/engine.hpp"
#include "utils/exceptions.hpp"

namespace tx {

/// Distributed base transaction engine. Has only common functionality shared
/// between the master and worker engines.
class EngineDistributed : public Engine {
  /// The wait time between two releases of local transaction objects that have
  /// expired on the master.
  static constexpr std::chrono::seconds kCacheReleasePeriod{1};

 public:
  /// Starts transactional cache cleanup. Transactional cache cleanup should be
  /// started *after* all of the registered objects are constructed to avoid
  /// segmentation faults.
  void StartTransactionalCacheCleanup() {
    cache_clearing_scheduler_.Run("TX cache GC", kCacheReleasePeriod, [this]() {
      std::lock_guard<std::mutex> guard(lock_);
      try {
        auto oldest_active = GlobalGcSnapshot().back();
        // Call all registered functions for cleanup.
        for (auto &f : functions_) f(oldest_active);
        // Clean our cache.
        ClearTransactionalCache(oldest_active);
      } catch (const utils::BasicException &e) {
        DLOG(WARNING)
            << "Couldn't perform transactional cache cleanup due to exception: "
            << e.what();
      }
    });
  }

  /// Registers the given object for transactional cleaning. The object will
  /// periodically get it's `ClearCache(tx::TransactionId)` method called
  /// with the oldest active transaction id. Note that the ONLY guarantee for
  /// the call param is that there are no transactions alive that have an id
  /// lower than it.
  template <typename TCache>
  void RegisterForTransactionalCacheCleanup(TCache &cache) {
    std::lock_guard<std::mutex> guard(lock_);
    functions_.emplace_back([&cache](tx::TransactionId oldest_active) {
      cache.ClearTransactionalCache(oldest_active);
    });
  }

  /// Stops transactional cache cleanup. Transactional cache cleanup should be
  /// stopped *before* all of the registered objects are destructed to avoid
  /// segmentation faults.
  void StopTransactionalCacheCleanup() { cache_clearing_scheduler_.Stop(); }

  /// Clears the cache of local transactions that have expired. This function
  /// has to be implemented by each class that inherits this class.
  virtual void ClearTransactionalCache(TransactionId oldest_active) = 0;

 private:
  std::mutex lock_;
  std::vector<std::function<void(tx::TransactionId)>> functions_;
  utils::Scheduler cache_clearing_scheduler_;
};
}  // namespace tx
