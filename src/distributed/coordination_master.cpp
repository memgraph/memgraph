#include <chrono>
#include <thread>

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {

MasterCoordination::MasterCoordination(const Endpoint &master_endpoint)
    : Coordination(master_endpoint) {}

bool MasterCoordination::RegisterWorker(int desired_worker_id,
                                        Endpoint endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  auto workers = GetWorkers();
  // Check if the desired worker id already exists.
  if (workers.find(desired_worker_id) != workers.end()) {
    LOG(WARNING) << "Unable to assign requested ID (" << desired_worker_id
                 << ") to worker at: " << endpoint;
    // If the desired worker ID is already assigned, return -1 and don't add
    // that worker to master coordination.
    return false;
  }

  AddWorker(desired_worker_id, endpoint);
  return true;
}

Endpoint MasterCoordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  return Coordination::GetEndpoint(worker_id);
}

MasterCoordination::~MasterCoordination() {
  using namespace std::chrono_literals;
  std::lock_guard<std::mutex> guard(lock_);
  auto workers = GetWorkers();
  for (const auto &kv : workers) {
    // Skip master (self).
    if (kv.first == 0) continue;
    communication::rpc::Client client(kv.second);
    auto result = client.Call<StopWorkerRpc>();
    CHECK(result) << "StopWorkerRpc failed for worker: " << kv.first;
  }

  // Make sure all StopWorkerRpc request/response are exchanged.
  std::this_thread::sleep_for(2s);
}

}  // namespace distributed
