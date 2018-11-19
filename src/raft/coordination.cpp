#include "glog/logging.h"

#include <thread>

#include "raft/coordination.hpp"

namespace raft {

Coordination::Coordination(
    uint16_t server_workers_count, uint16_t client_workers_count,
    uint16_t worker_id,
    std::unordered_map<uint16_t, io::network::Endpoint> workers)
    : server_(workers[worker_id], server_workers_count),
      workers_(workers),
      worker_id_(worker_id),
      thread_pool_(client_workers_count, "RPC client") {}

io::network::Endpoint Coordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = workers_.find(worker_id);
  CHECK(found != workers_.end())
      << "No endpoint registered for worker id: " << worker_id;
  return found->second;
}

io::network::Endpoint Coordination::GetServerEndpoint() {
  return server_.endpoint();
}

communication::rpc::ClientPool *Coordination::GetClientPool(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = client_pools_.find(worker_id);
  if (found != client_pools_.end()) return &found->second;
  auto found_endpoint = workers_.find(worker_id);
  CHECK(found_endpoint != workers_.end())
      << "No endpoint registered for worker id: " << worker_id;
  auto &endpoint = found_endpoint->second;
  return &client_pools_
              .emplace(std::piecewise_construct,
                       std::forward_as_tuple(worker_id),
                       std::forward_as_tuple(endpoint))
              .first->second;
}

void Coordination::AddWorker(int worker_id,
                             const io::network::Endpoint &endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  workers_.insert({worker_id, endpoint});
}

std::string Coordination::GetWorkerName(const io::network::Endpoint &endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  for (const auto &worker : workers_) {
    if (worker.second == endpoint) {
      if (worker.first == 0) {
        return fmt::format("master ({})", worker.second);
      } else {
        return fmt::format("worker {} ({})", worker.first, worker.second);
      }
    }
  }
  return fmt::format("unknown worker ({})", endpoint);
}

}  // namespace raft
