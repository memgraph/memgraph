#pragma once

#include <atomic>
#include <experimental/optional>
#include <functional>
#include <mutex>
#include <set>
#include <unordered_map>

#include "distributed/coordination.hpp"
#include "durability/distributed/recovery.hpp"
#include "io/network/endpoint.hpp"
#include "utils/scheduler.hpp"

namespace distributed {
using Endpoint = io::network::Endpoint;

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Master side. */
class MasterCoordination final : public Coordination {
 public:
  explicit MasterCoordination(
      const Endpoint &master_endpoint,
      int server_workers_count = std::thread::hardware_concurrency(),
      int client_workers_count = std::thread::hardware_concurrency());

  ~MasterCoordination();

  MasterCoordination(const MasterCoordination &) = delete;
  MasterCoordination(MasterCoordination &&) = delete;
  MasterCoordination &operator=(const MasterCoordination &) = delete;
  MasterCoordination &operator=(MasterCoordination &&) = delete;

  /** Registers a new worker with this master coordination.
   *
   * @param desired_worker_id - The ID the worker would like to have.
   * @return True if the desired ID for the worker is available, or false
   * if the desired ID is already taken.
   */
  bool RegisterWorker(int desired_worker_id, Endpoint endpoint);

  /*
   * Worker `worker_id` finished with recovering, adds it to the set of
   * recovered workers alongside with its recovery_info.
   */
  void WorkerRecoveredSnapshot(
      int worker_id, const std::experimental::optional<durability::RecoveryInfo>
                         &recovery_info);

  /// Sets the recovery info. nullopt indicates nothing was recovered.
  void SetRecoveredSnapshot(
      std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
          recovered_snapshot);

  std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
  RecoveredSnapshotTx() const;

  int CountRecoveredWorkers() const;

  std::vector<tx::TransactionId> CommonWalTransactions(
      const durability::RecoveryInfo &master_info) const;

  /// Starts the coordination and its servers.
  bool Start();

  /// Waits while the cluster is in a valid state or the `Shutdown` method is
  /// called (suitable for use with signal handlers). Blocks the calling thread
  /// until that has finished.
  /// @param call_before_shutdown function that should be called before
  /// shutdown, the function gets a bool argument indicating whether the cluster
  /// is alive and should return a bool indicating whether the shutdown
  /// succeeded without any issues
  /// @returns `true` if the shutdown was completed without any issues, `false`
  /// otherwise
  bool AwaitShutdown(std::function<bool(bool)> call_before_shutdown =
                         [](bool is_cluster_alive) -> bool { return true; });

  /// Hints that the coordination should start shutting down the whole cluster.
  void Shutdown();

  /// Returns `true` if the cluster is in a consistent state.
  bool IsClusterAlive();

 private:
  /// Sends a heartbeat request to all workers.
  void IssueHeartbeats();

  // Most master functions aren't thread-safe.
  mutable std::mutex master_lock_;

  // Durabilility recovery info.
  // Indicates if the recovery phase is done.
  bool recovery_done_{false};
  // Set of workers that finished sucesfully recovering snapshot
  std::map<int, std::experimental::optional<durability::RecoveryInfo>>
      recovered_workers_;
  // If nullopt nothing was recovered.
  std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
      recovered_snapshot_tx_;

  // Scheduler that is used to periodically ping all registered workers.
  utils::Scheduler scheduler_;

  // Flags used for shutdown.
  std::atomic<bool> alive_{true};
  std::atomic<bool> cluster_alive_{true};
};

}  // namespace distributed
