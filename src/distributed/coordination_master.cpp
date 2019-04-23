#include <algorithm>
#include <chrono>
#include <thread>

#include "glog/logging.h"

#include "communication/rpc/client.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "io/network/utils.hpp"
#include "utils/string.hpp"

namespace distributed {

// Send a heartbeat request to the workers every `kHeartbeatIntervalSeconds`.
// This constant must be at least 10x smaller than `kHeartbeatMaxDelaySeconds`
// that is defined in the worker coordination.
const int kHeartbeatIntervalSeconds = 1;

MasterCoordination::MasterCoordination(const Endpoint &master_endpoint,
                                       int server_workers_count,
                                       int client_workers_count)
    : Coordination(master_endpoint, 0, {}, server_workers_count,
                   client_workers_count) {}

MasterCoordination::~MasterCoordination() {
  CHECK(!alive_) << "You must call Shutdown and AwaitShutdown on "
                    "distributed::MasterCoordination!";
}

bool MasterCoordination::RegisterWorker(int desired_worker_id,
                                        Endpoint endpoint) {
  // Worker's can't register before the recovery phase on the master is done to
  // ensure the whole cluster is in a consistent state.
  while (true) {
    {
      std::lock_guard<std::mutex> guard(master_lock_);
      if (recovery_done_) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::lock_guard<std::mutex> guard(master_lock_);
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
    int worker_id,
    const std::optional<durability::RecoveryInfo> &recovery_info) {
  CHECK(recovered_workers_.insert(std::make_pair(worker_id, recovery_info))
            .second)
      << "Worker already notified about finishing recovery";
}

void MasterCoordination::SetRecoveredSnapshot(
    std::optional<std::pair<int64_t, tx::TransactionId>>
        recovered_snapshot_tx) {
  std::lock_guard<std::mutex> guard(master_lock_);
  recovery_done_ = true;
  recovered_snapshot_tx_ = recovered_snapshot_tx;
}

int MasterCoordination::CountRecoveredWorkers() const {
  return recovered_workers_.size();
}

std::optional<std::pair<int64_t, tx::TransactionId>>
MasterCoordination::RecoveredSnapshotTx() const {
  std::lock_guard<std::mutex> guard(master_lock_);
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
    std::lock_guard<std::mutex> guard(master_lock_);
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

bool MasterCoordination::Start() {
  if (!server_.Start()) return false;
  AddWorker(0, server_.endpoint());
  scheduler_.Run("Heartbeat", std::chrono::seconds(kHeartbeatIntervalSeconds),
                 [this] { IssueHeartbeats(); });
  return true;
}

bool MasterCoordination::AwaitShutdown(
    std::function<bool(bool)> call_before_shutdown) {
  // Wait for a shutdown notification.
  while (alive_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Copy the current value of the cluster state.
  bool is_cluster_alive = cluster_alive_;

  // Call the before shutdown callback.
  bool ret = call_before_shutdown(is_cluster_alive);

  // Stop the heartbeat scheduler so we don't cause any errors during shutdown.
  // Also, we manually issue one final heartbeat to all workers so that their
  // counters are reset. This must be done immediately before issuing shutdown
  // requests to the workers. The `IssueHeartbeats` will ignore any errors that
  // occur now because we are in the process of shutting the cluster down.
  scheduler_.Stop();
  IssueHeartbeats();

  // Shutdown all workers.
  auto workers = GetWorkers();
  std::vector<std::pair<int, io::network::Endpoint>> workers_sorted(
      workers.begin(), workers.end());
  std::sort(workers_sorted.begin(), workers_sorted.end(),
            [](const std::pair<int, io::network::Endpoint> &a,
               const std::pair<int, io::network::Endpoint> &b) {
              return a.first < b.first;
            });
  LOG(INFO) << "Starting shutdown of all workers.";
  for (const auto &worker : workers_sorted) {
    // Skip master (self).
    if (worker.first == 0) continue;
    auto client_pool = GetClientPool(worker.first);
    try {
      client_pool->Call<StopWorkerRpc>();
    } catch (const communication::rpc::RpcFailedException &e) {
      LOG(WARNING) << "Couldn't shutdown " << GetWorkerName(e.endpoint());
    }
  }

  // Make sure all workers have died.
  while (true) {
    std::vector<std::string> workers_alive;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &worker : workers_sorted) {
      // Skip master (self).
      if (worker.first == 0) continue;
      if (io::network::CanEstablishConnection(worker.second)) {
        workers_alive.push_back(GetWorkerName(worker.second));
      }
    }
    if (workers_alive.size() == 0) break;
    LOG(INFO) << "Waiting for " << utils::Join(workers_alive, ", ")
              << " to finish shutting down...";
  }
  LOG(INFO) << "Shutdown of all workers is complete.";

  // Some RPC servers might still depend on the cluster status to shut down. At
  // this point all workers are down which means that the cluster is also not
  // alive any more.
  cluster_alive_.store(false);

  // Shutdown our RPC server.
  server_.Shutdown();
  server_.AwaitShutdown();

  // Return `true` if the cluster is alive and the `call_before_shutdown`
  // succeeded.
  return ret && is_cluster_alive;
}

void MasterCoordination::Shutdown() { alive_.store(false); }

void MasterCoordination::IssueHeartbeats() {
  std::lock_guard<std::mutex> guard(master_lock_);
  auto workers = GetWorkers();
  for (const auto &worker : workers) {
    // Skip master (self).
    if (worker.first == 0) continue;
    auto client_pool = GetClientPool(worker.first);
    try {
      // TODO (mferencevic): Should we retry this call to ignore some transient
      // communication errors?
      client_pool->Call<HeartbeatRpc>();
    } catch (const communication::rpc::RpcFailedException &e) {
      // If we are not alive that means that we are in the process of a
      // shutdown. We ignore any exceptions here to stop our Heartbeat from
      // displaying warnings that the workers may have died (they should die,
      // we are shutting them down). Note: The heartbeat scheduler must stay
      // alive to ensure that the workers receive their heartbeat requests
      // during shutdown (which may take a long time).
      if (!alive_) continue;
      LOG(WARNING) << "The " << GetWorkerName(e.endpoint())
                   << " didn't respond to our heartbeat request. The cluster "
                      "is in a degraded state and we are starting a graceful "
                      "shutdown. Please check the logs on the worker for "
                      "more details.";
      // Set the `cluster_alive_` flag to `false` to indicate that something
      // in the cluster failed.
      cluster_alive_.store(false);
      // Shutdown the whole cluster.
      Shutdown();
    }
  }
}

}  // namespace distributed
