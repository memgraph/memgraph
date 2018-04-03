#include <chrono>
#include <thread>

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {

MasterCoordination::MasterCoordination(const Endpoint &master_endpoint)
    : Coordination(master_endpoint) {}

int MasterCoordination::RegisterWorker(int desired_worker_id,
                                       Endpoint endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  auto workers = GetWorkers();
  // If there is a desired worker ID, try to set it.
  if (desired_worker_id >= 0) {
    if (workers.find(desired_worker_id) == workers.end()) {
      AddWorker(desired_worker_id, endpoint);
      return desired_worker_id;
    }
    LOG(WARNING) << "Unable to assign requested ID (" << desired_worker_id
                 << ") to worker at: " << endpoint;
  }

  // Look for the next ID that's not used.
  int worker_id = 1;
  while (workers.find(worker_id) != workers.end()) ++worker_id;
  AddWorker(worker_id, endpoint);
  return worker_id;
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
