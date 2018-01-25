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
 public:
  RemotePullRpcClients(Coordination &coordination)
      : clients_(coordination, kRemotePullProduceRpcName) {}

  RemotePullResData RemotePull(tx::transaction_id_t tx_id, int worker_id,
                               int64_t plan_id, const Parameters &params,
                               const std::vector<query::Symbol> &symbols,
                               int batch_size = kDefaultBatchSize) {
    return std::move(clients_.GetClient(worker_id)
                         .Call<RemotePullRpc>(RemotePullReqData{
                             tx_id, plan_id, params, symbols, batch_size})
                         ->member);
  }

  // Notifies all workers that the given transaction/plan is done. Otherwise the
  // server is left with potentially unconsumed Cursors that never get deleted.
  //
  // TODO - maybe this needs to be done with hooks into the transactional
  // engine, so that the Worker discards it's stuff when the relevant
  // transaction are done.
  //
  // TODO - this will maybe need a per-worker granularity.
  void EndRemotePull(tx::transaction_id_t tx_id, int64_t plan_id) {
    auto futures = clients_.ExecuteOnWorkers<void>(
        0, [tx_id, plan_id](communication::rpc::Client &client) {
          client.Call<EndRemotePullRpc>(EndRemotePullReqData{tx_id, plan_id});
        });
    for (auto &future : futures) future.wait();
  }

 private:
  RpcWorkerClients clients_;
};

}  // namespace distributed
