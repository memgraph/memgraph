#pragma once

#include <mutex>
#include <unordered_map>

#include "communication/rpc/server.hpp"
#include "distributed/coordination.hpp"

namespace distributed {

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Worker side. */
class WorkerCoordination final : public Coordination {
  using Endpoint = io::network::Endpoint;

 public:
  WorkerCoordination(communication::rpc::Server &server,
                     const Endpoint &master_endpoint);

  /** Registers the worker with the given endpoint. */
  int RegisterWorker(int worker_id, Endpoint endpoint);

  /** Starts listening for a remote shutdown command (issued by the master).
   * Blocks the calling thread until that has finished. */
  void WaitForShutdown();

  Endpoint GetEndpoint(int worker_id);

 private:
  communication::rpc::Server &server_;
  mutable std::mutex lock_;
};
}  // namespace distributed
