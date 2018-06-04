#include <unordered_map>

#include "distributed/data_rpc_clients.hpp"
#include "distributed/data_rpc_messages.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

template <>
std::unique_ptr<Edge> DataRpcClients::RemoteElement(int worker_id,
                                                    tx::TransactionId tx_id,
                                                    gid::Gid gid) {
  auto response =
      clients_.GetClientPool(worker_id).Call<EdgeRpc>(TxGidPair{tx_id, gid});
  CHECK(response) << "EdgeRpc failed";
  return std::move(response->edge_output);
}

template <>
std::unique_ptr<Vertex> DataRpcClients::RemoteElement(int worker_id,
                                                      tx::TransactionId tx_id,
                                                      gid::Gid gid) {
  auto response =
      clients_.GetClientPool(worker_id).Call<VertexRpc>(TxGidPair{tx_id, gid});
  CHECK(response) << "VertexRpc failed";
  return std::move(response->vertex_output);
}

std::unordered_map<int, int64_t> DataRpcClients::VertexCounts(
    tx::TransactionId tx_id) {
  auto future_results = clients_.ExecuteOnWorkers<std::pair<int, int64_t>>(
      -1, [tx_id](int worker_id, communication::rpc::ClientPool &client_pool) {
        auto response = client_pool.Call<VertexCountRpc>(tx_id);
        CHECK(response) << "VertexCountRpc failed";
        return std::make_pair(worker_id, response->member);
      });

  std::unordered_map<int, int64_t> results;
  for (auto &result : future_results) {
    auto result_pair = result.get();
    int worker = result_pair.first;
    int vertex_count = result_pair.second;
    results[worker] = vertex_count;
  }
  return results;
}

}  // namespace distributed
