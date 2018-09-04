#pragma once

#include <functional>
#include <type_traits>
#include <unordered_map>

#include "communication/rpc/client_pool.hpp"
#include "distributed/coordination.hpp"
#include "distributed/index_rpc_messages.hpp"
#include "distributed/token_sharing_rpc_messages.hpp"
#include "storage/types.hpp"
#include "transactions/transaction.hpp"
#include "utils/future.hpp"
#include "utils/thread.hpp"

namespace distributed {

/** A cache of RPC clients (of the given name/kind) per MG distributed worker.
 * Thread safe. */
class RpcWorkerClients {
 public:
  explicit RpcWorkerClients(Coordination &coordination)
      : coordination_(coordination),
        thread_pool_(std::thread::hardware_concurrency()) {}

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
      std::function<TResult(int worker_id, communication::rpc::ClientPool &)>
          execute) {
    auto &client_pool = GetClientPool(worker_id);
    return thread_pool_.Run(execute, worker_id, std::ref(client_pool));
  }

  /** Asynchroniously executes the `execute` function on all worker rpc clients
   * except the one whose id is `skip_worker_id`. Returns a vectore of futures
   * contaning the results of the `execute` function. */
  template <typename TResult>
  auto ExecuteOnWorkers(
      int skip_worker_id,
      std::function<TResult(int worker_id, communication::rpc::ClientPool &)>
          execute) {
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
  utils::ThreadPool thread_pool_;
};

/** Wrapper class around a RPC call to build indices.
 */
class IndexRpcClients {
 public:
  explicit IndexRpcClients(RpcWorkerClients &clients) : clients_(clients) {}

  auto GetPopulateIndexFutures(const storage::Label &label,
                               const storage::Property &property,
                               tx::TransactionId transaction_id,
                               int worker_id) {
    return clients_.ExecuteOnWorkers<bool>(
        worker_id,
        [label, property, transaction_id](
            int worker_id, communication::rpc::ClientPool &client_pool) {
          return static_cast<bool>(client_pool.Call<PopulateIndexRpc>(
              label, property, transaction_id));
        });
  }

  auto GetCreateIndexFutures(const storage::Label &label,
                             const storage::Property &property, int worker_id) {
    return clients_.ExecuteOnWorkers<bool>(
        worker_id,
        [label, property](int worker_id,
                          communication::rpc::ClientPool &client_pool) {
          return static_cast<bool>(
              client_pool.Call<CreateIndexRpc>(label, property));
        });
  }

 private:
  RpcWorkerClients &clients_;
};

/** Wrapper class around a RPC call to share token between workers.
 */
class TokenSharingRpcClients {
 public:
  explicit TokenSharingRpcClients(RpcWorkerClients *clients)
      : clients_(clients) {}

  auto TransferToken(int worker_id) {
    return clients_->ExecuteOnWorker<void>(
        worker_id,
        [](int worker_id, communication::rpc::ClientPool &client_pool) {
          CHECK(client_pool.Call<TokenTransferRpc>())
              << "Unable to transfer token";
        });
  }

 private:
  RpcWorkerClients *clients_;
};

}  // namespace distributed
