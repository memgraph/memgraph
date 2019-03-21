/// @file
#pragma once

#include <experimental/optional>

#include "storage/distributed/storage_gc.hpp"
#include "transactions/type.hpp"

namespace database {

/// A common base for wrappers around storage garbage collector in distributed.
/// This class uses composition on `StorageGc` to enable reinitialization.
/// Reinitialization is needed when we fail to recover from a snapshot and we
/// reinitialize the underlying storage object and thus we must reinitialize the
/// garbage collector.
class StorageGcDistributed {
 public:
  StorageGcDistributed() = delete;
  StorageGcDistributed(Storage *storage, tx::Engine *tx_engine, int pause_sec)
      : storage_gc_(std::make_unique<StorageGc>(
            storage, tx_engine, pause_sec,
            [this](tx::TransactionId oldest_active, tx::Engine *tx_engine) {
              this->CollectCommitLogGarbage(oldest_active, tx_engine);
            })),
        pause_sec_(pause_sec) {}

  virtual ~StorageGcDistributed() {
    CHECK(!storage_gc_->IsRunning())
        << "You must call Stop on database::StorageGcMaster!";
  }

  virtual void CollectCommitLogGarbage(tx::TransactionId oldest_active,
                                       tx::Engine *tx_engine) = 0;

  virtual void Reinitialize(Storage *storage, tx::Engine *tx_engine) {
    storage_gc_ = nullptr;
    storage_gc_ = std::make_unique<StorageGc>(
        storage, tx_engine, pause_sec_,
        [this](tx::TransactionId oldest_active, tx::Engine *tx_engine) {
          this->CollectCommitLogGarbage(oldest_active, tx_engine);
        });
  }

  void Stop() { storage_gc_->Stop(); }

  void CollectGarbage() { storage_gc_->CollectGarbage(); }

  std::experimental::optional<tx::TransactionId> GetClogSafeTransaction(
      tx::TransactionId oldest_active) {
    return storage_gc_->GetClogSafeTransaction(oldest_active);
  }

 private:
  std::unique_ptr<StorageGc> storage_gc_;
  int pause_sec_;
};

}  // namespace database
