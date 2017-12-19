#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "io/network/network_endpoint.hpp"

namespace distributed {
using Endpoint = io::network::NetworkEndpoint;

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Worker side. */
class WorkerCoordination {
 public:
  WorkerCoordination(communication::messaging::System &system,
                     const Endpoint &master_endpoint);

  /**
   * Registers a worker with the master.
   *
   * @param worker_id - Desired ID. If -1, or if the desired ID is already
   * taken, the worker gets the next available ID.
   */
  int RegisterWorker(int desired_worker_id = -1);

  /** Gets the endpoint for the given worker ID from the master. */
  Endpoint GetEndpoint(int worker_id);

  /** Starts listening for a remote shutdown command (issued by the master).
   * Blocks the calling thread until that has finished. */
  void WaitForShutdown();

  /** Shuts the RPC server down. */
  void Shutdown();

 private:
  communication::messaging::System &system_;
  communication::rpc::Client client_;
  communication::rpc::Server server_;
  ConcurrentMap<int, Endpoint> endpoint_cache_;
};
}  // namespace distributed
