#pragma once

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "distributed/coordination_worker.hpp"

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

 private:
  Server &server_;
  WorkerCoordination &coordination_;
  communication::rpc::ClientPool &client_pool_;
};

}  // namespace distributed
