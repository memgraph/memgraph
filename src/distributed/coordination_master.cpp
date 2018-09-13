#include <chrono>
#include <thread>

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "io/network/utils.hpp"

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

void MasterCoordination::WorkerRecoveredSnapshot(
    int worker_id, const std::experimental::optional<durability::RecoveryInfo>
                       &recovery_info) {
  CHECK(recovered_workers_.insert(std::make_pair(worker_id, recovery_info))
            .second)
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
    while (io::network::CanEstablishConnection(kv.second))
      std::this_thread::sleep_for(0.5s);
  }
}

void MasterCoordination::SetRecoveredSnapshot(
    std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
        recovered_snapshot_tx) {
  std::lock_guard<std::mutex> guard(lock_);
  recovery_done_ = true;
  recovered_snapshot_tx_ = recovered_snapshot_tx;
}

int MasterCoordination::CountRecoveredWorkers() const {
  return recovered_workers_.size();
}

std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
MasterCoordination::RecoveredSnapshotTx() const {
  std::lock_guard<std::mutex> guard(lock_);
  CHECK(recovery_done_) << "Recovered snapshot requested before it's available";
  return recovered_snapshot_tx_;
}

std::vector<tx::TransactionId> MasterCoordination::CommonWalTransactions(
    const durability::RecoveryInfo &master_info) const {
  int cluster_size;
  std::unordered_map<tx::TransactionId, int> tx_cnt;
  for (auto tx : master_info.wal_recovered) {
    tx_cnt[tx]++;
  }

  {
    std::lock_guard<std::mutex> guard(lock_);
    for (auto worker : recovered_workers_) {
      // If there is no recovery info we can just return an empty vector since
      // we can't restore any transaction
      if (!worker.second) return {};
      for (auto tx : worker.second->wal_recovered) {
        tx_cnt[tx]++;
      }
    }
    // Add one because of master
    cluster_size = recovered_workers_.size() + 1;
  }

  std::vector<tx::TransactionId> tx_intersection;
  for (auto tx : tx_cnt) {
    if (tx.second == cluster_size) {
      tx_intersection.push_back(tx.first);
    }
  }

  return tx_intersection;
}

}  // namespace distributed
