#pragma once

#include <experimental/optional>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "distributed/coordination_worker.hpp"
#include "durability/recovery.hpp"

namespace distributed {
using Server = communication::rpc::Server;
using ClientPool = communication::rpc::ClientPool;

/** Handle cluster discovery on worker.
 *
 * Cluster discovery on worker handles worker registration by sending an rpc
 * request to master and processes received rpc response with other worker
 * information.
 */
class ClusterDiscoveryWorker final {
 public:
  ClusterDiscoveryWorker(Server &server, WorkerCoordination &coordination,
                         ClientPool &client_pool);

  /**
   * Registers a worker with the master.
   *
   * @param worker_id - Desired ID. If master can't assign the desired worker
   * id, worker will exit.
   */
  void RegisterWorker(int worker_id);

  /**
   * Notifies the master that the worker finished recovering. Assumes that the
   * worker was already registered with master.
   */
  void NotifyWorkerRecovered(
      const std::experimental::optional<durability::RecoveryInfo>
          &recovery_info);

  /** Returns the snapshot that should be recovered on workers. Valid only after
   * registration. */
  auto snapshot_to_recover() const { return snapshot_to_recover_; }

 private:
  int worker_id_{-1};
  Server &server_;
  WorkerCoordination &coordination_;
  communication::rpc::ClientPool &client_pool_;
  std::experimental::optional<tx::TransactionId> snapshot_to_recover_;
};

}  // namespace distributed
