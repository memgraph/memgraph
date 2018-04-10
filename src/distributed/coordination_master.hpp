#pragma once

#include <mutex>
#include <unordered_map>

#include "distributed/coordination.hpp"
#include "io/network/endpoint.hpp"

namespace distributed {
using Endpoint = io::network::Endpoint;

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Master side. */
class MasterCoordination final : public Coordination {
 public:
  explicit MasterCoordination(const Endpoint &master_endpoint);

  /** Shuts down all the workers and this master server. */
  ~MasterCoordination();

  /** Registers a new worker with this master coordination.
   *
   * @param desired_worker_id - The ID the worker would like to have.
   * @return True if the desired ID for the worker is available, or false
   * if the desired ID is already taken.
   */
  bool RegisterWorker(int desired_worker_id, Endpoint endpoint);

  Endpoint GetEndpoint(int worker_id);

 private:
  // Most master functions aren't thread-safe.
  mutable std::mutex lock_;
};
}  // namespace distributed
