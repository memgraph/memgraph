#pragma once

#include <future>
#include <mutex>
#include <utility>

#include "distributed/coordination.hpp"
#include "durability/recovery.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Provides an ability to trigger snapshooting on other workers.
class DurabilityRpcMaster {
 public:
  explicit DurabilityRpcMaster(Coordination *coordination)
      : coordination_(coordination) {}

  // Sends a snapshot request to workers and returns a future which becomes true
  // if all workers sucesfully completed their snapshot creation, false
  // otherwise
  // @param tx - transaction from which to take db snapshot
  utils::Future<bool> MakeSnapshot(tx::TransactionId tx);

  utils::Future<bool> RecoverWalAndIndexes(
      durability::RecoveryData *recovery_data);

 private:
  Coordination *coordination_;
};

}  // namespace distributed
