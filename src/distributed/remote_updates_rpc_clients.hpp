#pragma once

#include "database/state_delta.hpp"
#include "distributed/coordination.hpp"
#include "distributed/remote_updates_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Exposes the functionality to send updates to other workers (that own the
/// graph element we are updating). Also enables us to call for a worker to
/// apply the accumulated deferred updates, or discard them.
class RemoteUpdatesRpcClients {
 public:
  explicit RemoteUpdatesRpcClients(distributed::Coordination &coordination)
      : worker_clients_(coordination, kRemoteUpdatesRpc) {}

  /// Sends an update delta to the given worker.
  RemoteUpdateResult RemoteUpdate(int worker_id,
                                  const database::StateDelta &delta) {
    return worker_clients_.GetClientPool(worker_id)
        .Call<RemoteUpdateRpc>(delta)
        ->member;
  }

  /// Calls for the worker with the given ID to apply remote updates. Returns
  /// the results of that operation.
  RemoteUpdateResult RemoteUpdateApply(int worker_id,
                                       tx::transaction_id_t tx_id) {
    return worker_clients_.GetClientPool(worker_id)
        .Call<RemoteUpdateApplyRpc>(tx_id)
        ->member;
  }

  /// Calls for all the workers (except the given one) to apply their updates
  /// and returns the future results.
  std::vector<std::future<RemoteUpdateResult>> RemoteUpdateApplyAll(
      int skip_worker_id, tx::transaction_id_t tx_id) {
    return worker_clients_.ExecuteOnWorkers<RemoteUpdateResult>(
        skip_worker_id, [tx_id](auto &client) {
          return client.template Call<RemoteUpdateApplyRpc>(tx_id)->member;
        });
  }

  /// Calls for the worker with the given ID to discard remote updates.
  void RemoteUpdateDiscard(int worker_id, tx::transaction_id_t tx_id) {
    worker_clients_.GetClientPool(worker_id).Call<RemoteUpdateDiscardRpc>(
        tx_id);
  }

 private:
  RpcWorkerClients worker_clients_;
};
}  // namespace distributed
