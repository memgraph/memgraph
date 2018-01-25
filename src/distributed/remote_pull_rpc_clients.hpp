#pragma once

#include <functional>
#include <vector>

#include "distributed/remote_pull_produce_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/parameters.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Provides means of calling for the execution of a plan on some remote worker,
 * and getting the results of that execution. The results are returned in
 * batches and are therefore accompanied with an enum indicator of the state of
 * remote execution. */
class RemotePullRpcClients {
  using ClientPool = communication::rpc::ClientPool;

 public:
  RemotePullRpcClients(Coordination &coordination)
      : clients_(coordination, kRemotePullProduceRpcName) {}

  /// Calls a remote pull asynchroniously. IMPORTANT: take care not to call this
  /// function for the same (tx_id, worker_id, plan_id) before the previous call
  /// has ended.
  std::future<RemotePullResData> RemotePull(
      tx::transaction_id_t tx_id, int worker_id, int64_t plan_id,
      const Parameters &params, const std::vector<query::Symbol> &symbols,
      int batch_size = kDefaultBatchSize) {
    return clients_.ExecuteOnWorker<RemotePullResData>(
        worker_id, [tx_id, plan_id, &params, &symbols,
                    batch_size](ClientPool &client_pool) {
          return client_pool
              .Call<RemotePullRpc>(RemotePullReqData{tx_id, plan_id, params,
                                                     symbols, batch_size})
              ->member;
        });
  }

  auto GetWorkerIds() { return clients_.GetWorkerIds(); }

  // Notifies a worker that the given transaction/plan is done. Otherwise the
  // server is left with potentially unconsumed Cursors that never get deleted.
  //
  // TODO - maybe this needs to be done with hooks into the transactional
  // engine, so that the Worker discards it's stuff when the relevant
  // transaction are done.
  std::future<void> EndRemotePull(int worker_id, tx::transaction_id_t tx_id,
                                  int64_t plan_id) {
    return clients_.ExecuteOnWorker<void>(
        worker_id, [tx_id, plan_id](ClientPool &client_pool) {
          return client_pool.Call<EndRemotePullRpc>(
              EndRemotePullReqData{tx_id, plan_id});
        });
  }

  void EndAllRemotePulls(tx::transaction_id_t tx_id, int64_t plan_id) {
    std::vector<std::future<void>> futures;
    for (auto worker_id : clients_.GetWorkerIds()) {
      if (worker_id == 0) continue;
      futures.emplace_back(EndRemotePull(worker_id, tx_id, plan_id));
    }
    for (auto &future : futures) future.wait();
  }

 private:
  RpcWorkerClients clients_;
};

}  // namespace distributed
