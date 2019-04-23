#pragma once

#include <optional>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "distributed/coordination_worker.hpp"
#include "durability/distributed/recovery.hpp"

namespace distributed {

/** Handle cluster discovery on worker.
 *
 * Cluster discovery on worker handles worker registration by sending an rpc
 * request to master and processes received rpc response with other worker
 * information.
 */
class ClusterDiscoveryWorker final {
 public:
  ClusterDiscoveryWorker(WorkerCoordination *coordination);

  /**
   * Registers a worker with the master.
   *
   * @param worker_id - Desired ID. If master can't assign the desired worker
   * id, worker will exit.
   * @param durability_directory - The durability directory that is used for
   * this worker.
   */
  void RegisterWorker(int worker_id, const std::string &durability_directory);

  /**
   * Notifies the master that the worker finished recovering. Assumes that the
   * worker was already registered with master.
   */
  void NotifyWorkerRecovered(
      const std::optional<durability::RecoveryInfo> &recovery_info);

  /** Returns the snapshot that should be recovered on workers. Valid only after
   * registration. */
  auto snapshot_to_recover() const { return snapshot_to_recover_; }

 private:
  int worker_id_{-1};
  distributed::WorkerCoordination *coordination_;
  communication::rpc::ClientPool *client_pool_;
  std::optional<std::pair<int64_t, tx::TransactionId>> snapshot_to_recover_;
};

}  // namespace distributed
