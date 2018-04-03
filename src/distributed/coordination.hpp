#pragma once

#include <unordered_map>
#include <vector>

#include "io/network/endpoint.hpp"

namespace distributed {

/** Coordination base class. This class is not thread safe. */
class Coordination {
 public:
  explicit Coordination(const io::network::Endpoint &master_endpoint);

  /** Gets the endpoint for the given worker ID from the master. */
  io::network::Endpoint GetEndpoint(int worker_id);

  /** Returns all workers id, this includes master id(0). */
  std::vector<int> GetWorkerIds() const;

  /** Gets the mapping of worker id to worker endpoint including master (worker
   * id = 0).
   */
  std::unordered_map<int, io::network::Endpoint> GetWorkers();

 protected:
  ~Coordination() {}

  /** Adds a worker to coordination. */
  void AddWorker(int worker_id, io::network::Endpoint endpoint);

 private:
  std::unordered_map<int, io::network::Endpoint> workers_;
};

}  // namespace distributed
