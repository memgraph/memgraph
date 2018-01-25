#pragma once

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/coordination.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Worker side. */
class WorkerCoordination : public Coordination {
  using Endpoint = io::network::Endpoint;

 public:
  WorkerCoordination(communication::rpc::System &system,
                     const Endpoint &master_endpoint);

  /**
   * Registers a worker with the master.
   *
   * @param worker_id - Desired ID. If -1, or if the desired ID is already
   * taken, the worker gets the next available ID.
   */
  int RegisterWorker(int desired_worker_id = -1);

  /** Gets the endpoint for the given worker ID from the master. */
  Endpoint GetEndpoint(int worker_id) override;

  /** Shouldn't be called on worker for now!
   * TODO fix this */
  std::vector<int> GetWorkerIds() override;

  /** Starts listening for a remote shutdown command (issued by the master).
   * Blocks the calling thread until that has finished. */
  void WaitForShutdown();

 private:
  communication::rpc::System &system_;
  communication::rpc::ClientPool client_pool_;
  communication::rpc::Server server_;
  mutable ConcurrentMap<int, Endpoint> endpoint_cache_;
};
}  // namespace distributed
