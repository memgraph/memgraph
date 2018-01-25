#pragma once

#include <functional>
#include <future>
#include <type_traits>
#include <unordered_map>

#include "communication/rpc/client_pool.hpp"
#include "distributed/coordination.hpp"

namespace distributed {

/** A cache of RPC clients (of the given name/kind) per MG distributed worker.
 * Thread safe. */
class RpcWorkerClients {
 public:
  RpcWorkerClients(Coordination &coordination,
                   const std::string &rpc_client_name)
      : coordination_(coordination), rpc_client_name_(rpc_client_name) {}

  RpcWorkerClients(const RpcWorkerClients &) = delete;
  RpcWorkerClients(RpcWorkerClients &&) = delete;
  RpcWorkerClients &operator=(const RpcWorkerClients &) = delete;
  RpcWorkerClients &operator=(RpcWorkerClients &&) = delete;

  auto &GetClientPool(int worker_id) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = client_pools_.find(worker_id);
    if (found != client_pools_.end()) return found->second;
    return client_pools_
        .emplace(std::piecewise_construct, std::forward_as_tuple(worker_id),
                 std::forward_as_tuple(coordination_.GetEndpoint(worker_id),
                                       rpc_client_name_))
        .first->second;
  }

  auto GetWorkerIds() { return coordination_.GetWorkerIds(); }

  /** Asynchroniously executes the given function on the rpc client for the
   * given worker id. Returns an `std::future` of the given `execute` function's
   * return type. */
  template <typename TResult>
  auto ExecuteOnWorker(
      int worker_id,
      std::function<TResult(communication::rpc::ClientPool &)> execute) {
    auto &client = GetClientPool(worker_id);
    return std::async(std::launch::async,
                      [execute, &client]() { return execute(client); });
  }

  /** Asynchroniously executes the `execute` function on all worker rpc clients
   * except the one whose id is `skip_worker_id`. Returns a vectore of futures
   * contaning the results of the `execute` function. */
  template <typename TResult>
  auto ExecuteOnWorkers(
      int skip_worker_id,
      std::function<TResult(communication::rpc::ClientPool &)> execute) {
    std::vector<std::future<TResult>> futures;
    for (auto &worker_id : coordination_.GetWorkerIds()) {
      if (worker_id == skip_worker_id) continue;
      futures.emplace_back(std::move(ExecuteOnWorker(worker_id, execute)));
    }
    return futures;
  }

 private:
  // TODO make Coordination const, it's member GetEndpoint must be const too.
  Coordination &coordination_;
  const std::string rpc_client_name_;
  std::unordered_map<int, communication::rpc::ClientPool> client_pools_;
  std::mutex lock_;
};

}  // namespace distributed
