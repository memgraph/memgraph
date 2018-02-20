#pragma once

#include <unordered_map>
#include <vector>

#include "database/state_delta.hpp"
#include "distributed/coordination.hpp"
#include "distributed/remote_updates_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "query/typed_value.hpp"
#include "storage/address_types.hpp"
#include "storage/gid.hpp"
#include "storage/types.hpp"
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

  /// Creates a vertex on the given worker and returns it's id.
  gid::Gid RemoteCreateVertex(
      int worker_id, tx::transaction_id_t tx_id,
      const std::vector<storage::Label> &labels,
      const std::unordered_map<storage::Property, query::TypedValue>
          &properties) {
    auto result =
        worker_clients_.GetClientPool(worker_id).Call<RemoteCreateVertexRpc>(
            RemoteCreateVertexReqData{tx_id, labels, properties});
    CHECK(result) << "Failed to remote-create a vertex on worker: "
                  << worker_id;
    CHECK(result->member.result == RemoteUpdateResult::DONE)
        << "Vertex creation can not result in an error";
    return result->member.gid;
  }

  /// Creates an edge on the given worker and returns it's address. If the `to`
  /// vertex is on the same worker as `from`, then all remote CRUD will be
  /// handled by a call to this function. Otherwise a separate call to
  /// `RemoteAddInEdge` might be necessary. Throws all the exceptions that can
  /// occur remotely as a result of updating a vertex.
  storage::EdgeAddress RemoteCreateEdge(tx::transaction_id_t tx_id,
                                        VertexAccessor &from,
                                        VertexAccessor &to,
                                        storage::EdgeType edge_type) {
    CHECK(from.address().is_remote())
        << "In RemoteCreateEdge `from` must be remote";

    int from_worker = from.address().worker_id();
    auto res = worker_clients_.GetClientPool(from_worker)
                   .Call<RemoteCreateEdgeRpc>(RemoteCreateEdgeReqData{
                       from.gid(), to.GlobalAddress(), edge_type, tx_id});
    CHECK(res) << "RemoteCreateEdge RPC failed";
    RaiseIfRemoteError(res->member.result);
    return {res->member.gid, from_worker};
  }

  /// Adds the edge with the given address to the `to` vertex as an incoming
  /// edge. Only used when `to` is remote and not on the same worker as `from`.
  void RemoteAddInEdge(tx::transaction_id_t tx_id, VertexAccessor &from,
                       storage::EdgeAddress edge_address, VertexAccessor &to,
                       storage::EdgeType edge_type) {
    CHECK(to.address().is_remote() && edge_address.is_remote() &&
          (from.GlobalAddress().worker_id() != to.address().worker_id()))
        << "RemoteAddInEdge should only be called when `to` is remote and "
           "`from` is not on the same worker as `to`.";
    auto res = worker_clients_.GetClientPool(to.GlobalAddress().worker_id())
                   .Call<RemoteAddInEdgeRpc>(RemoteAddInEdgeReqData{
                       from.GlobalAddress(), edge_address, to.gid(), edge_type,
                       tx_id});
    CHECK(res) << "RemoteAddInEdge RPC failed";
    RaiseIfRemoteError(res->member);
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

 private:
  RpcWorkerClients worker_clients_;

  void RaiseIfRemoteError(RemoteUpdateResult result) {
    switch (result) {
      case RemoteUpdateResult::SERIALIZATION_ERROR:
        throw new mvcc::SerializationError();
      case RemoteUpdateResult::LOCK_TIMEOUT_ERROR:
        throw new LockTimeoutException(
            "Remote LockTimeoutError during edge creation");
      case RemoteUpdateResult::UPDATE_DELETED_ERROR:
        throw new RecordDeletedError();
      case RemoteUpdateResult::DONE:
        break;
    }
  }
};
}  // namespace distributed
