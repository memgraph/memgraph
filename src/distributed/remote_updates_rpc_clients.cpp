
#include <unordered_map>
#include <vector>

#include "distributed/remote_updates_rpc_clients.hpp"
#include "query/exceptions.hpp"

namespace distributed {

namespace {
void RaiseIfRemoteError(RemoteUpdateResult result) {
  switch (result) {
    case RemoteUpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR:
      throw query::RemoveAttachedVertexException();
    case RemoteUpdateResult::SERIALIZATION_ERROR:
      throw mvcc::SerializationError();
    case RemoteUpdateResult::LOCK_TIMEOUT_ERROR:
      throw LockTimeoutException(
          "Remote LockTimeoutError during edge creation");
    case RemoteUpdateResult::UPDATE_DELETED_ERROR:
      throw RecordDeletedError();
    case RemoteUpdateResult::DONE:
      break;
  }
}
}

RemoteUpdateResult RemoteUpdatesRpcClients::RemoteUpdate(
    int worker_id, const database::StateDelta &delta) {
  auto res =
      worker_clients_.GetClientPool(worker_id).Call<RemoteUpdateRpc>(delta);
  CHECK(res) << "RemoteUpdateRpc failed on worker: " << worker_id;
  return res->member;
}

gid::Gid RemoteUpdatesRpcClients::RemoteCreateVertex(
    int worker_id, tx::transaction_id_t tx_id,
    const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, query::TypedValue>
        &properties) {
  auto res =
      worker_clients_.GetClientPool(worker_id).Call<RemoteCreateVertexRpc>(
          RemoteCreateVertexReqData{tx_id, labels, properties});
  CHECK(res) << "RemoteCreateVertexRpc failed on worker: " << worker_id;
  CHECK(res->member.result == RemoteUpdateResult::DONE)
      << "Remote Vertex creation result not RemoteUpdateResult::DONE";
  return res->member.gid;
}

storage::EdgeAddress RemoteUpdatesRpcClients::RemoteCreateEdge(
    tx::transaction_id_t tx_id, VertexAccessor &from, VertexAccessor &to,
    storage::EdgeType edge_type) {
  CHECK(from.address().is_remote())
      << "In RemoteCreateEdge `from` must be remote";

  int from_worker = from.address().worker_id();
  auto res = worker_clients_.GetClientPool(from_worker)
                 .Call<RemoteCreateEdgeRpc>(RemoteCreateEdgeReqData{
                     from.gid(), to.GlobalAddress(), edge_type, tx_id});
  CHECK(res) << "RemoteCreateEdge RPC failed on worker: " << from_worker;
  RaiseIfRemoteError(res->member.result);
  return {res->member.gid, from_worker};
}

void RemoteUpdatesRpcClients::RemoteAddInEdge(tx::transaction_id_t tx_id,
                                              VertexAccessor &from,
                                              storage::EdgeAddress edge_address,
                                              VertexAccessor &to,
                                              storage::EdgeType edge_type) {
  CHECK(to.address().is_remote() && edge_address.is_remote() &&
        (from.GlobalAddress().worker_id() != to.address().worker_id()))
      << "RemoteAddInEdge should only be called when `to` is remote and "
         "`from` is not on the same worker as `to`.";
  auto worker_id = to.GlobalAddress().worker_id();
  auto res = worker_clients_.GetClientPool(worker_id).Call<RemoteAddInEdgeRpc>(
      RemoteAddInEdgeReqData{from.GlobalAddress(), edge_address, to.gid(),
                             edge_type, tx_id});
  CHECK(res) << "RemoteAddInEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void RemoteUpdatesRpcClients::RemoteRemoveVertex(int worker_id,
                                                 tx::transaction_id_t tx_id,
                                                 gid::Gid gid,
                                                 bool check_empty) {
  auto res =
      worker_clients_.GetClientPool(worker_id).Call<RemoteRemoveVertexRpc>(
          RemoteRemoveVertexReqData{gid, tx_id, check_empty});
  CHECK(res) << "RemoteRemoveVertex RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void RemoteUpdatesRpcClients::RemoteRemoveEdge(
    tx::transaction_id_t tx_id, int worker_id, gid::Gid edge_gid,
    gid::Gid vertex_from_id, storage::VertexAddress vertex_to_addr) {
  auto res = worker_clients_.GetClientPool(worker_id).Call<RemoteRemoveEdgeRpc>(
      RemoteRemoveEdgeData{tx_id, edge_gid, vertex_from_id, vertex_to_addr});
  CHECK(res) << "RemoteRemoveEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void RemoteUpdatesRpcClients::RemoteRemoveInEdge(
    tx::transaction_id_t tx_id, int worker_id, gid::Gid vertex_id,
    storage::EdgeAddress edge_address) {
  CHECK(edge_address.is_remote())
      << "RemoteRemoveInEdge edge_address is local.";
  auto res =
      worker_clients_.GetClientPool(worker_id).Call<RemoteRemoveInEdgeRpc>(
          RemoteRemoveInEdgeData{tx_id, vertex_id, edge_address});
  CHECK(res) << "RemoteRemoveInEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

std::vector<utils::Future<RemoteUpdateResult>>
RemoteUpdatesRpcClients::RemoteUpdateApplyAll(int skip_worker_id,
                                              tx::transaction_id_t tx_id) {
  return worker_clients_.ExecuteOnWorkers<RemoteUpdateResult>(
      skip_worker_id, [tx_id](auto &client) {
        auto res = client.template Call<RemoteUpdateApplyRpc>(tx_id);
        CHECK(res) << "RemoteUpdateApplyRpc failed";
        return res->member;
      });
}

}  // namespace distributed
