#include <chrono>
#include <thread>

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "utils/network.hpp"

namespace distributed {

MasterCoordination::MasterCoordination(const Endpoint &master_endpoint)
    : Coordination(master_endpoint) {}

bool MasterCoordination::RegisterWorker(int desired_worker_id,
                                        Endpoint endpoint) {
  // Worker's can't register before the recovery phase on the master is done to
  // ensure the whole cluster is in a consistent state.
  while (true) {
    {
      std::lock_guard<std::mutex> guard(lock_);
      if (recovery_done_) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

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

void MasterCoordination::WorkerRecovered(int worker_id) {
  CHECK(recovered_workers_.insert(worker_id).second)
      << "Worker already notified about finishing recovery";
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

  // Make sure all workers have died.
  for (const auto &kv : workers) {
    // Skip master (self).
    if (kv.first == 0) continue;
    while (utils::CanEstablishConnection(kv.second))
      std::this_thread::sleep_for(0.5s);
  }
}

void MasterCoordination::SetRecoveryInfo(
    std::experimental::optional<durability::RecoveryInfo> info) {
  std::lock_guard<std::mutex> guard(lock_);
  recovery_done_ = true;
  recovery_info_ = info;
}

int MasterCoordination::CountRecoveredWorkers() const {
  return recovered_workers_.size();
}

std::experimental::optional<durability::RecoveryInfo>
MasterCoordination::RecoveryInfo() const {
  std::lock_guard<std::mutex> guard(lock_);
  CHECK(recovery_done_) << "RecoveryInfo requested before it's available";
  return recovery_info_;
}

}  // namespace distributed
