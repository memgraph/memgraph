#include "distributed/remote_data_rpc_clients.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

template <>
std::unique_ptr<Edge> RemoteDataRpcClients::RemoteElement(
    int worker_id, tx::transaction_id_t tx_id, gid::Gid gid) {
  auto response = clients_.GetClientPool(worker_id).Call<RemoteEdgeRpc>(
      TxGidPair{tx_id, gid});
  CHECK(response) << "RemoteEdgeRpc failed";
  return std::move(response->name_output_);
}

template <>
std::unique_ptr<Vertex> RemoteDataRpcClients::RemoteElement(
    int worker_id, tx::transaction_id_t tx_id, gid::Gid gid) {
  auto response = clients_.GetClientPool(worker_id).Call<RemoteVertexRpc>(
      TxGidPair{tx_id, gid});
  CHECK(response) << "RemoteVertexRpc failed";
  return std::move(response->name_output_);
}

}  // namespace distributed
