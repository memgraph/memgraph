#pragma once

#include <functional>
#include <type_traits>
#include <unordered_map>

#include "communication/rpc/client_pool.hpp"
#include "distributed/coordination.hpp"
#include "distributed/index_rpc_messages.hpp"
#include "storage/types.hpp"
#include "transactions/transaction.hpp"
#include "utils/future.hpp"

namespace distributed {

/** A cache of RPC clients (of the given name/kind) per MG distributed worker.
 * Thread safe. */
class RpcWorkerClients {
 public:
  RpcWorkerClients(Coordination &coordination) : coordination_(coordination) {}

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
                 std::forward_as_tuple(coordination_.GetEndpoint(worker_id)))
        .first->second;
  }

  auto GetWorkerIds() { return coordination_.GetWorkerIds(); }

  /** Asynchroniously executes the given function on the rpc client for the
   * given worker id. Returns an `utils::Future` of the given `execute`
   * function's
   * return type. */
  template <typename TResult>
  auto ExecuteOnWorker(
      int worker_id,
      std::function<TResult(communication::rpc::ClientPool &)> execute) {
    auto &client_pool = GetClientPool(worker_id);
    return utils::make_future(
        std::async(std::launch::async,
                   [execute, &client_pool]() { return execute(client_pool); }));
  }

  /** Asynchroniously executes the `execute` function on all worker rpc clients
   * except the one whose id is `skip_worker_id`. Returns a vectore of futures
   * contaning the results of the `execute` function. */
  template <typename TResult>
  auto ExecuteOnWorkers(
      int skip_worker_id,
      std::function<TResult(communication::rpc::ClientPool &)> execute) {
    std::vector<utils::Future<TResult>> futures;
    for (auto &worker_id : coordination_.GetWorkerIds()) {
      if (worker_id == skip_worker_id) continue;
      futures.emplace_back(std::move(ExecuteOnWorker(worker_id, execute)));
    }
    return futures;
  }

 private:
  // TODO make Coordination const, it's member GetEndpoint must be const too.
  Coordination &coordination_;
  std::unordered_map<int, communication::rpc::ClientPool> client_pools_;
  std::mutex lock_;
};

/** Wrapper class around a RPC call to build indices.
 */
class IndexRpcClients {
 public:
  IndexRpcClients(RpcWorkerClients &clients) : clients_(clients) {}

  auto GetBuildIndexFutures(const storage::Label &label,
                            const storage::Property &property,
                            tx::transaction_id_t transaction_id,
                            int worker_id) {
    return clients_.ExecuteOnWorkers<bool>(
        worker_id, [label, property, transaction_id](
                       communication::rpc::ClientPool &client_pool) {
          return client_pool.Call<BuildIndexRpc>(
                     distributed::IndexLabelPropertyTx{
                         label, property, transaction_id}) != nullptr;
        });
  }

 private:
  RpcWorkerClients &clients_;
};
}  // namespace distributed
