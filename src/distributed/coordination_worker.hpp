#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>

#include "communication/rpc/server.hpp"
#include "distributed/coordination.hpp"
#include "utils/scheduler.hpp"

namespace distributed {

/// Handles worker registration, getting of other workers' endpoints and
/// coordinated shutdown in a distributed memgraph. Worker side.
class WorkerCoordination final : public Coordination {
 public:
  WorkerCoordination(
      const io::network::Endpoint &worker_endpoint, int worker_id,
      const io::network::Endpoint &master_endpoint,
      int server_workers_count = std::thread::hardware_concurrency(),
      int client_workers_count = std::thread::hardware_concurrency());

  ~WorkerCoordination();

  WorkerCoordination(const WorkerCoordination &) = delete;
  WorkerCoordination(WorkerCoordination &&) = delete;
  WorkerCoordination &operator=(const WorkerCoordination &) = delete;
  WorkerCoordination &operator=(WorkerCoordination &&) = delete;

  /// Registers the worker with the given endpoint.
  void RegisterWorker(int worker_id, io::network::Endpoint endpoint);

  /// Starts the coordination and its servers.
  bool Start();

  /// Starts listening for a remote shutdown command (issued by the master) or
  /// for the `Shutdown` method to be called (suitable for use with signal
  /// handlers). Blocks the calling thread until that has finished.
  /// @param call_before_shutdown function that should be called before
  /// shutdown, the function gets a bool argument indicating whether the cluster
  /// is alive and should return a bool indicating whether the shutdown
  /// succeeded without any issues
  /// @returns `true` if the shutdown was completed without any issues, `false`
  /// otherwise
  bool AwaitShutdown(std::function<bool(bool)> call_before_shutdown =
                         [](bool is_cluster_alive) -> bool { return true; });

  /// Hints that the coordination should start shutting down the worker.
  void Shutdown();

 private:
  // Heartbeat variables
  std::mutex heartbeat_lock_;
  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_time_;
  utils::Scheduler scheduler_;

  // Flag used for shutdown.
  std::atomic<bool> alive_{true};
  std::atomic<bool> cluster_alive_{true};
};
}  // namespace distributed
