#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {

MasterCoordination::MasterCoordination(communication::messaging::System &system)
    : system_(system), server_(system, kCoordinationServerName) {
  server_.Register<RegisterWorkerRpc>([this](const RegisterWorkerReq &req) {
    auto worker_id = RegisterWorker(req.desired_worker_id, req.endpoint);
    return std::make_unique<RegisterWorkerRes>(worker_id);
  });
  server_.Register<GetEndpointRpc>([this](const GetEndpointReq &req) {
    return std::make_unique<GetEndpointRes>(GetEndpoint(req.member));
  });
  server_.Start();
}

int MasterCoordination::RegisterWorker(int desired_worker_id,
                                       Endpoint endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  int worker_id = desired_worker_id;
  // Check if the desired ID is available.
  if (workers_.find(worker_id) != workers_.end()) {
    if (desired_worker_id >= 0)
      LOG(WARNING) << "Unable to assign requested ID (" << worker_id
                   << ") to worker at \"" << endpoint.address() << ":"
                   << endpoint.port() << "\"";
    worker_id = 1;
  }
  // Look for the next ID that's not used.
  while (workers_.find(worker_id) != workers_.end()) ++worker_id;
  workers_.emplace(worker_id, endpoint);
  return worker_id;
}

void MasterCoordination::Shutdown() {
  std::lock_guard<std::mutex> guard(lock_);
  for (const auto &kv : workers_) {
    communication::rpc::Client client(system_, kv.second,
                                      kCoordinationServerName);
    auto result = client.Call<StopWorkerRpc>(100ms);
    CHECK(result) << "Failed to shut down worker: " << kv.first;
  }
  server_.Shutdown();
}

Endpoint MasterCoordination::GetEndpoint(int worker_id) const {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = workers_.find(worker_id);
  CHECK(found != workers_.end()) << "No endpoint registered for worker id: "
                                 << worker_id;
  return found->second;
}
}  // namespace distributed
