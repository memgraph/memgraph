#pragma once

#include <functional>
#include <future>
#include <type_traits>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "distributed/coordination.hpp"

namespace distributed {

/** A cache of RPC clients (of the given name/kind) per MG distributed worker.
 * Thread safe. */
class RpcWorkerClients {
 public:
  RpcWorkerClients(communication::messaging::System &system,
                   Coordination &coordination,
                   const std::string &rpc_client_name)
      : system_(system),
        coordination_(coordination),
        rpc_client_name_(rpc_client_name) {}

  RpcWorkerClients(const RpcWorkerClients &) = delete;
  RpcWorkerClients(RpcWorkerClients &&) = delete;
  RpcWorkerClients &operator=(const RpcWorkerClients &) = delete;
  RpcWorkerClients &operator=(RpcWorkerClients &&) = delete;

  auto &GetClient(int worker_id) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = clients_.find(worker_id);
    if (found != clients_.end()) return found->second;
    return clients_
        .emplace(
            std::piecewise_construct, std::forward_as_tuple(worker_id),
            std::forward_as_tuple(system_, coordination_.GetEndpoint(worker_id),
                                  rpc_client_name_))
        .first->second;
  }

  auto GetWorkerIds() { return coordination_.GetWorkerIds(); }

  /**
   * Promises to execute function on workers rpc clients.
   * @Tparam TResult - deduced automatically from method
   * @param skip_worker_id - worker which to skip (set to -1 to avoid skipping)
   * @param execute - Method which takes an rpc client and returns a result for
   * it
   * @return list of futures filled with function 'execute' results when applied
   * to rpc clients
   */
  template <typename TResult>
  auto ExecuteOnWorkers(
      int skip_worker_id,
      std::function<TResult(communication::rpc::Client &)> execute) {
    std::vector<std::future<TResult>> futures;
    for (auto &worker_id : coordination_.GetWorkerIds()) {
      if (worker_id == skip_worker_id) continue;
      auto &client = GetClient(worker_id);

      futures.emplace_back(
          std::async(std::launch::async,
                     [&execute, &client]() { return execute(client); }));
    }
    return futures;
  }

 private:
  communication::messaging::System &system_;
  // TODO make Coordination const, it's member GetEndpoint must be const too.
  Coordination &coordination_;
  const std::string rpc_client_name_;
  std::unordered_map<int, communication::rpc::Client> clients_;
  std::mutex lock_;
};

}  // namespace distributed
