#include "glog/logging.h"

#include "distributed/coordination.hpp"

namespace distributed {
using Endpoint = io::network::Endpoint;

Coordination::Coordination(const Endpoint &master_endpoint) {
  // The master is always worker 0.
  workers_.emplace(0, master_endpoint);
}

Endpoint Coordination::GetEndpoint(int worker_id) {
  auto found = workers_.find(worker_id);
  CHECK(found != workers_.end()) << "No endpoint registered for worker id: "
                                 << worker_id;
  return found->second;
}

std::vector<int> Coordination::GetWorkerIds() const {
  std::vector<int> worker_ids;
  for (auto worker : workers_) worker_ids.push_back(worker.first);
  return worker_ids;
}

void Coordination::AddWorker(int worker_id, Endpoint endpoint) {
  workers_.emplace(worker_id, endpoint);
}

std::unordered_map<int, Endpoint> Coordination::GetWorkers() {
  return workers_;
}

}  // namespace distributed
