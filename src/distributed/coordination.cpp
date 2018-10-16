#include "glog/logging.h"

#include <thread>

#include "distributed/coordination.hpp"

namespace distributed {

Coordination::Coordination(const io::network::Endpoint &worker_endpoint,
                           int worker_id,
                           const io::network::Endpoint &master_endpoint,
                           int server_workers_count, int client_workers_count)
    : server_(worker_endpoint, server_workers_count),
      worker_id_(worker_id),
      thread_pool_(client_workers_count, "RPC client") {
  // The master endpoint should be added to workers and not to the master.
  if (worker_id != 0) {
    // The master is always worker 0.
    // TODO (mferencevic): Strictly speaking, this isn't correct when creating a
    // `CoordinationMaster` because the supplied `master_endpoint` is supplied
    // before the server starts listening on the address. Eg. if `0.0.0.0:0` is
    // supplied that should be first resolved by the server when it binds to
    // that address and `server_.endpoint()` should be used. Currently,
    // `workers_[0]` isn't used on the master so all is well.
    workers_.emplace(0, master_endpoint);
  } else {
    workers_.emplace(0, server_.endpoint());
  }
}

Coordination::~Coordination() {}

io::network::Endpoint Coordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = workers_.find(worker_id);
  // TODO (mferencevic): Handle this error situation differently.
  CHECK(found != workers_.end())
      << "No endpoint registered for worker id: " << worker_id;
  return found->second;
}

io::network::Endpoint Coordination::GetServerEndpoint() {
  return server_.endpoint();
}

std::vector<int> Coordination::GetWorkerIds() {
  std::lock_guard<std::mutex> guard(lock_);
  std::vector<int> worker_ids;
  for (auto worker : workers_) worker_ids.push_back(worker.first);
  return worker_ids;
}

std::unordered_map<int, io::network::Endpoint> Coordination::GetWorkers() {
  std::lock_guard<std::mutex> guard(lock_);
  return workers_;
}

communication::rpc::ClientPool *Coordination::GetClientPool(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = client_pools_.find(worker_id);
  if (found != client_pools_.end()) return &found->second;
  auto found_endpoint = workers_.find(worker_id);
  // TODO (mferencevic): Handle this error situation differently.
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

}  // namespace distributed
