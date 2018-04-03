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
   * @param worker_id - Desired ID. If -1, or if the desired ID is already
   * taken, the worker gets the next available ID.
   */
  int RegisterWorker(int desired_worker_id = -1);

 private:
  Server &server_;
  WorkerCoordination &coordination_;
  communication::rpc::ClientPool &client_pool_;
};

}  // namespace distributed
