#include "distributed/data_rpc_clients.hpp"
#include "distributed/data_rpc_messages.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

template <>
std::unique_ptr<Edge> DataRpcClients::RemoteElement(int worker_id,
                                                    tx::transaction_id_t tx_id,
                                                    gid::Gid gid) {
  auto response =
      clients_.GetClientPool(worker_id).Call<EdgeRpc>(TxGidPair{tx_id, gid});
  CHECK(response) << "EdgeRpc failed";
  return std::move(response->name_output_);
}

template <>
std::unique_ptr<Vertex> DataRpcClients::RemoteElement(
    int worker_id, tx::transaction_id_t tx_id, gid::Gid gid) {
  auto response =
      clients_.GetClientPool(worker_id).Call<VertexRpc>(TxGidPair{tx_id, gid});
  CHECK(response) << "VertexRpc failed";
  return std::move(response->name_output_);
}

}  // namespace distributed
