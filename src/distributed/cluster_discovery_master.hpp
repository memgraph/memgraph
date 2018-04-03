#pragma once

#include "communication/rpc/server.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/rpc_worker_clients.hpp"

namespace distributed {
using Server = communication::rpc::Server;

/** Handle cluster discovery on master.
 *
 * Cluster discovery on master handles worker registration and broadcasts new
 * worker information to already registered workers, and already registered
 * worker information to the new worker.
 */
class ClusterDiscoveryMaster final {
 public:
  ClusterDiscoveryMaster(Server &server, MasterCoordination &coordination,
                         RpcWorkerClients &rpc_worker_clients);

 private:
  Server &server_;
  MasterCoordination &coordination_;
  RpcWorkerClients &rpc_worker_clients_;
};

}  // namespace distributed
