#pragma once

#include <functional>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "communication/rpc/client_pool.hpp"
#include "io/network/endpoint.hpp"
#include "utils/future.hpp"
#include "utils/thread.hpp"

namespace distributed {

/// Coordination base class. This class is thread safe.
class Coordination {
 protected:
  Coordination(const io::network::Endpoint &master_endpoint, int worker_id,
               int client_workers_count = std::thread::hardware_concurrency());
  ~Coordination();

 public:
  /// Gets the endpoint for the given worker ID from the master.
  io::network::Endpoint GetEndpoint(int worker_id);

  /// Returns all workers id, this includes master (ID 0).
  std::vector<int> GetWorkerIds();

  /// Gets the mapping of worker id to worker endpoint including master (ID 0).
  std::unordered_map<int, io::network::Endpoint> GetWorkers();

  /// Returns a cached `ClientPool` for the given `worker_id`.
  communication::rpc::ClientPool *GetClientPool(int worker_id);

  /// Asynchroniously executes the given function on the rpc client for the
  /// given worker id. Returns an `utils::Future` of the given `execute`
  /// function's return type.
  template <typename TResult>
  auto ExecuteOnWorker(
      int worker_id,
      std::function<TResult(int worker_id, communication::rpc::ClientPool &)>
          execute) {
    // TODO (mferencevic): Change this lambda to accept a pointer to
    // `ClientPool` instead of a reference!
    auto client_pool = GetClientPool(worker_id);
    return thread_pool_.Run(execute, worker_id, std::ref(*client_pool));
  }

  /// Asynchroniously executes the `execute` function on all worker rpc clients
  /// except the one whose id is `skip_worker_id`. Returns a vector of futures
  /// contaning the results of the `execute` function.
  template <typename TResult>
  auto ExecuteOnWorkers(
      int skip_worker_id,
      std::function<TResult(int worker_id, communication::rpc::ClientPool &)>
          execute) {
    std::vector<utils::Future<TResult>> futures;
    // TODO (mferencevic): GetWorkerIds always copies the vector of workers,
    // this may be an issue...
    for (auto &worker_id : GetWorkerIds()) {
      if (worker_id == skip_worker_id) continue;
      futures.emplace_back(std::move(ExecuteOnWorker(worker_id, execute)));
    }
    return futures;
  }

 protected:
  /// Adds a worker to the coordination. This function can be called multiple
  /// times to replace an existing worker.
  void AddWorker(int worker_id, const io::network::Endpoint &endpoint);

  /// Gets a worker name for the given endpoint.
  std::string GetWorkerName(const io::network::Endpoint &endpoint);

 private:
  std::unordered_map<int, io::network::Endpoint> workers_;
  mutable std::mutex lock_;
  int worker_id_;

  std::unordered_map<int, communication::rpc::ClientPool> client_pools_;
  utils::ThreadPool thread_pool_;
};

}  // namespace distributed
