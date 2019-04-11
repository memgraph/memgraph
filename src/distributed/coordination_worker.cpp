#include <chrono>
#include <mutex>
#include <thread>

#include "glog/logging.h"

#include "distributed/coordination_rpc_messages.hpp"
#include "distributed/coordination_worker.hpp"

namespace distributed {

// Expect that a heartbeat should be received in this time interval. If it is
// not received we assume that the communication is broken and start a shutdown.
const int kHeartbeatMaxDelaySeconds = 10;

// Check whether a heartbeat is received every `kHeartbeatCheckSeconds`. It
// should be larger than `kHeartbeatIntervalSeconds` defined in the master
// coordination because it makes no sense to check more often than the heartbeat
// is sent. Also, it must be smaller than `kHeartbeatMaxDelaySeconds` to
// function properly.
const int kHeartbeatCheckSeconds = 2;

using namespace std::chrono_literals;

WorkerCoordination::WorkerCoordination(
    const io::network::Endpoint &worker_endpoint, int worker_id,
    const io::network::Endpoint &master_endpoint, int server_workers_count,
    int client_workers_count)
    : Coordination(worker_endpoint, worker_id, master_endpoint,
                   server_workers_count, client_workers_count) {
  server_.Register<StopWorkerRpc>(
      [&](const auto &req_reader, auto *res_builder) {
        LOG(INFO) << "The master initiated shutdown of this worker.";
        Shutdown();
      });

  server_.Register<HeartbeatRpc>([&](const auto &req_reader,
                                     auto *res_builder) {
    std::lock_guard<std::mutex> guard(heartbeat_lock_);
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    if (!scheduler_.IsRunning()) {
      scheduler_.Run(
          "Heartbeat", std::chrono::seconds(kHeartbeatCheckSeconds), [this] {
            std::lock_guard<std::mutex> guard(heartbeat_lock_);
            auto duration =
                std::chrono::steady_clock::now() - last_heartbeat_time_;
            if (duration > std::chrono::seconds(kHeartbeatMaxDelaySeconds)) {
              LOG(WARNING) << "The master hasn't given us a heartbeat request "
                              "for at least "
                           << kHeartbeatMaxDelaySeconds
                           << " seconds! We are shutting down...";
              // Set the `cluster_alive_` flag to `false` to indicate that
              // something in the cluster failed.
              cluster_alive_ = false;
              // Shutdown the worker.
              Shutdown();
            }
          });
    }
  });
}

WorkerCoordination::~WorkerCoordination() {
  CHECK(!alive_) << "You must call Shutdown and AwaitShutdown on "
                    "distributed::WorkerCoordination!";
}

void WorkerCoordination::RegisterWorker(int worker_id,
                                        io::network::Endpoint endpoint) {
  AddWorker(worker_id, endpoint);
}

bool WorkerCoordination::Start() {
  return server_.Start();
}

bool WorkerCoordination::AwaitShutdown(
    std::function<bool(bool)> call_before_shutdown) {
  // Wait for a shutdown notification.
  while (alive_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // The first thing we need to do is to stop our heartbeat scheduler because
  // the master stopped their scheduler immediately before issuing the shutdown
  // request to the worker. This will prevent our heartbeat from timing out on a
  // regular shutdown.
  scheduler_.Stop();

  // Copy the current value of the cluster state.
  bool is_cluster_alive = cluster_alive_;

  // Call the before shutdown callback.
  bool ret = call_before_shutdown(is_cluster_alive);

  // Shutdown our RPC server.
  server_.Shutdown();
  server_.AwaitShutdown();

  // All other cleanup must be done here.

  // Return `true` if the cluster is alive and the `call_before_shutdown`
  // succeeded.
  return ret && is_cluster_alive;
}

void WorkerCoordination::Shutdown() { alive_.store(false); }

}  // namespace distributed
