
#include <unordered_map>
#include <vector>

#include "distributed/updates_rpc_clients.hpp"
#include "query/exceptions.hpp"

namespace distributed {

namespace {
void RaiseIfRemoteError(UpdateResult result) {
  switch (result) {
    case UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR:
      throw query::RemoveAttachedVertexException();
    case UpdateResult::SERIALIZATION_ERROR:
      throw mvcc::SerializationError();
    case UpdateResult::LOCK_TIMEOUT_ERROR:
      throw LockTimeoutException(
          "Remote LockTimeoutError during edge creation");
    case UpdateResult::UPDATE_DELETED_ERROR:
      throw RecordDeletedError();
    case UpdateResult::DONE:
      break;
  }
}
}

UpdateResult UpdatesRpcClients::Update(int worker_id,
                                       const database::StateDelta &delta) {
  auto res = worker_clients_.GetClientPool(worker_id).Call<UpdateRpc>(delta);
  CHECK(res) << "UpdateRpc failed on worker: " << worker_id;
  return res->member;
}

gid::Gid UpdatesRpcClients::CreateVertex(
    int worker_id, tx::TransactionId tx_id,
    const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, query::TypedValue>
        &properties) {
  auto res = worker_clients_.GetClientPool(worker_id).Call<CreateVertexRpc>(
      CreateVertexReqData{tx_id, labels, properties});
  CHECK(res) << "CreateVertexRpc failed on worker: " << worker_id;
  CHECK(res->member.result == UpdateResult::DONE)
      << "Remote Vertex creation result not UpdateResult::DONE";
  return res->member.gid;
}

storage::EdgeAddress UpdatesRpcClients::CreateEdge(
    tx::TransactionId tx_id, VertexAccessor &from, VertexAccessor &to,
    storage::EdgeType edge_type) {
  CHECK(from.address().is_remote()) << "In CreateEdge `from` must be remote";

  int from_worker = from.address().worker_id();
  auto res = worker_clients_.GetClientPool(from_worker)
                 .Call<CreateEdgeRpc>(CreateEdgeReqData{
                     from.gid(), to.GlobalAddress(), edge_type, tx_id});
  CHECK(res) << "CreateEdge RPC failed on worker: " << from_worker;
  RaiseIfRemoteError(res->member.result);
  return {res->member.gid, from_worker};
}

void UpdatesRpcClients::AddInEdge(tx::TransactionId tx_id,
                                  VertexAccessor &from,
                                  storage::EdgeAddress edge_address,
                                  VertexAccessor &to,
                                  storage::EdgeType edge_type) {
  CHECK(to.address().is_remote() && edge_address.is_remote() &&
        (from.GlobalAddress().worker_id() != to.address().worker_id()))
      << "AddInEdge should only be called when `to` is remote and "
         "`from` is not on the same worker as `to`.";
  auto worker_id = to.GlobalAddress().worker_id();
  auto res = worker_clients_.GetClientPool(worker_id).Call<AddInEdgeRpc>(
      AddInEdgeReqData{from.GlobalAddress(), edge_address, to.gid(), edge_type,
                       tx_id});
  CHECK(res) << "AddInEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void UpdatesRpcClients::RemoveVertex(int worker_id, tx::TransactionId tx_id,
                                     gid::Gid gid, bool check_empty) {
  auto res = worker_clients_.GetClientPool(worker_id).Call<RemoveVertexRpc>(
      RemoveVertexReqData{gid, tx_id, check_empty});
  CHECK(res) << "RemoveVertex RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void UpdatesRpcClients::RemoveEdge(tx::TransactionId tx_id, int worker_id,
                                   gid::Gid edge_gid, gid::Gid vertex_from_id,
                                   storage::VertexAddress vertex_to_addr) {
  auto res = worker_clients_.GetClientPool(worker_id).Call<RemoveEdgeRpc>(
      RemoveEdgeData{tx_id, edge_gid, vertex_from_id, vertex_to_addr});
  CHECK(res) << "RemoveEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

void UpdatesRpcClients::RemoveInEdge(tx::TransactionId tx_id, int worker_id,
                                     gid::Gid vertex_id,
                                     storage::EdgeAddress edge_address) {
  CHECK(edge_address.is_remote()) << "RemoveInEdge edge_address is local.";
  auto res = worker_clients_.GetClientPool(worker_id).Call<RemoveInEdgeRpc>(
      RemoveInEdgeData{tx_id, vertex_id, edge_address});
  CHECK(res) << "RemoveInEdge RPC failed on worker: " << worker_id;
  RaiseIfRemoteError(res->member);
}

std::vector<utils::Future<UpdateResult>> UpdatesRpcClients::UpdateApplyAll(
    int skip_worker_id, tx::TransactionId tx_id) {
  return worker_clients_.ExecuteOnWorkers<UpdateResult>(
      skip_worker_id, [tx_id](int worker_id, auto &client) {
        auto res = client.template Call<UpdateApplyRpc>(tx_id);
        CHECK(res) << "UpdateApplyRpc failed";
        return res->member;
      });
}

}  // namespace distributed
