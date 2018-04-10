#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "glog/logging.h"

#include "distributed/coordination_rpc_messages.hpp"
#include "distributed/coordination_worker.hpp"

namespace distributed {

using namespace std::literals::chrono_literals;

WorkerCoordination::WorkerCoordination(communication::rpc::Server &server,
                                       const Endpoint &master_endpoint)
    : Coordination(master_endpoint), server_(server) {}

void WorkerCoordination::RegisterWorker(int worker_id, Endpoint endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  AddWorker(worker_id, endpoint);
}

void WorkerCoordination::WaitForShutdown() {
  using namespace std::chrono_literals;
  std::mutex mutex;
  std::condition_variable cv;
  bool shutdown = false;

  server_.Register<StopWorkerRpc>([&](const StopWorkerReq &) {
    std::unique_lock<std::mutex> lk(mutex);
    shutdown = true;
    lk.unlock();
    cv.notify_one();
    return std::make_unique<StopWorkerRes>();
  });

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait(lk, [&shutdown] { return shutdown; });
  // Sleep to allow the server to return the StopWorker response. This is
  // necessary because Shutdown will most likely be called after this function.
  // TODO (review): Should we call server_.Shutdown() here? Not the usual
  // convention, but maybe better...
  std::this_thread::sleep_for(100ms);
};

Endpoint WorkerCoordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  return Coordination::GetEndpoint(worker_id);
}

}  // namespace distributed
