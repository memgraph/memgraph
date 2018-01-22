#pragma once

#include <mutex>
#include <utility>

#include "communication/messaging/distributed.hpp"
#include "distributed/coordination.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Provides access to other worker's data. */
class RemoteDataRpcClients {
  using Client = communication::rpc::Client;

 public:
  RemoteDataRpcClients(communication::messaging::System &system,
                       Coordination &coordination)
      : clients_(system, coordination, kRemoteDataRpcName) {}

  /// Returns a remote worker's data for the given params. That worker must own
  /// the vertex for the given id, and that vertex must be visible in given
  /// transaction.
  std::unique_ptr<Vertex> RemoteVertex(int worker_id,
                                       tx::transaction_id_t tx_id,
                                       gid::Gid gid) {
    auto response = clients_.GetClient(worker_id).Call<RemoteVertexRpc>(
        kRemoteDataRpcTimeout, TxGidPair{tx_id, gid});
    return std::move(response->name_output_);
  }

  /// Returns a remote worker's data for the given params. That worker must own
  /// the edge for the given id, and that edge must be visible in given
  /// transaction.
  std::unique_ptr<Edge> RemoteEdge(int worker_id, tx::transaction_id_t tx_id,
                                   gid::Gid gid) {
    auto response = clients_.GetClient(worker_id).Call<RemoteEdgeRpc>(
        kRemoteDataRpcTimeout, TxGidPair{tx_id, gid});
    return std::move(response->name_output_);
  }

  template <typename TRecord>
  std::unique_ptr<TRecord> RemoteElement(int worker_id,
                                         tx::transaction_id_t tx_id,
                                         gid::Gid gid);

 private:
  RpcWorkerClients clients_;
};

template <>
inline std::unique_ptr<Edge> RemoteDataRpcClients::RemoteElement(
    int worker_id, tx::transaction_id_t tx_id, gid::Gid gid) {
  return RemoteEdge(worker_id, tx_id, gid);
}

template <>
inline std::unique_ptr<Vertex> RemoteDataRpcClients::RemoteElement(
    int worker_id, tx::transaction_id_t tx_id, gid::Gid gid) {
  return RemoteVertex(worker_id, tx_id, gid);
}

}  // namespace distributed
