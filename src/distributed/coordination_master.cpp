#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {

MasterCoordination::MasterCoordination(communication::rpc::System &system)
    : server_(system, kCoordinationServerName) {
  // The master is always worker 0.
  workers_.emplace(0, system.endpoint());

  server_.Register<RegisterWorkerRpc>([this](const RegisterWorkerReq &req) {
    auto worker_id = RegisterWorker(req.desired_worker_id, req.endpoint);
    return std::make_unique<RegisterWorkerRes>(worker_id);
  });
  server_.Register<GetEndpointRpc>([this](const GetEndpointReq &req) {
    return std::make_unique<GetEndpointRes>(GetEndpoint(req.member));
  });
}

int MasterCoordination::RegisterWorker(int desired_worker_id,
                                       Endpoint endpoint) {
  std::lock_guard<std::mutex> guard(lock_);

  // If there is a desired worker ID, try to set it.
  if (desired_worker_id >= 0) {
    if (workers_.find(desired_worker_id) == workers_.end()) {
      workers_.emplace(desired_worker_id, endpoint);
      return desired_worker_id;
    }
    LOG(WARNING) << "Unable to assign requested ID (" << desired_worker_id
                 << ") to worker at: " << endpoint;
  }

  // Look for the next ID that's not used.
  int worker_id = 1;
  while (workers_.find(worker_id) != workers_.end()) ++worker_id;
  workers_.emplace(worker_id, endpoint);
  return worker_id;
}

MasterCoordination::~MasterCoordination() {
  std::lock_guard<std::mutex> guard(lock_);
  for (const auto &kv : workers_) {
    // Skip master (self).
    if (kv.first == 0) continue;
    communication::rpc::Client client(kv.second, kCoordinationServerName);
    auto result = client.Call<StopWorkerRpc>();
    CHECK(result) << "Failed to shut down worker: " << kv.first;
  }
}

Endpoint MasterCoordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = workers_.find(worker_id);
  CHECK(found != workers_.end())
      << "No endpoint registered for worker id: " << worker_id;
  return found->second;
}

std::vector<int> MasterCoordination::GetWorkerIds() {
  std::vector<int> worker_ids;
  for (auto worker : workers_) worker_ids.push_back(worker.first);
  return worker_ids;
}

}  // namespace distributed
