#pragma once

#include "storage/single_node/storage_gc.hpp"

namespace database {
class StorageGcSingleNode : public StorageGc {
 public:
  using StorageGc::StorageGc;

  ~StorageGcSingleNode() {
    // We have to stop scheduler before destroying this class because otherwise
    // a task might try to utilize methods in this class which might cause pure
    // virtual method called since they are not implemented for the base class.
    scheduler_.Stop();
  }

  void CollectCommitLogGarbage(tx::TransactionId oldest_active) final {
    auto safe_to_delete = GetClogSafeTransaction(oldest_active);
    if (safe_to_delete) tx_engine_.GarbageCollectCommitLog(*safe_to_delete);
  }
};
}  // namespace database
