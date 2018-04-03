#pragma once

#include "database/storage_gc.hpp"

namespace database {
class StorageGcSingleNode : public StorageGc {
 public:
  using StorageGc::StorageGc;

  void CollectCommitLogGarbage(tx::transaction_id_t oldest_active) final {
    auto safe_to_delete = GetClogSafeTransaction(oldest_active);
    if (safe_to_delete) tx_engine_.GarbageCollectCommitLog(*safe_to_delete);
  }
};
}  // namespace database
