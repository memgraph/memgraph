#pragma once

#include <mutex>
#include <utility>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "database/state_delta.hpp"
#include "distributed/coordination.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Provides access to other worker's data. */
class RemoteDataRpcClients {
  using Client = communication::rpc::Client;

 public:
  RemoteDataRpcClients(communication::messaging::System &system,
                       Coordination &coordination)
      : system_(system), coordination_(coordination) {}

  /// Returns a remote worker's data for the given params. That worker must own
  /// the vertex for the given id, and that vertex must be visible in given
  /// transaction.
  std::unique_ptr<Vertex> RemoteVertex(int worker_id,
                                       tx::transaction_id_t tx_id,
                                       gid::Gid gid) {
    auto response = RemoteDataClient(worker_id).Call<RemoteVertexRpc>(
        kRemoteDataRpcTimeout, TxGidPair{tx_id, gid});
    return std::move(response->name_output_);
  }

  /// Returns a remote worker's data for the given params. That worker must own
  /// the edge for the given id, and that edge must be visible in given
  /// transaction.
  std::unique_ptr<Edge> RemoteEdge(int worker_id, tx::transaction_id_t tx_id,
                                   gid::Gid gid) {
    auto response = RemoteDataClient(worker_id).Call<RemoteEdgeRpc>(
        kRemoteDataRpcTimeout, TxGidPair{tx_id, gid});
    return std::move(response->name_output_);
  }

  template <typename TRecord>
  std::unique_ptr<TRecord> RemoteElement(int worker_id,
                                         tx::transaction_id_t tx_id,
                                         gid::Gid gid);

 private:
  communication::messaging::System &system_;
  // TODO make Coordination const, it's member GetEndpoint must be const too.
  Coordination &coordination_;

  std::unordered_map<int, Client> clients_;
  std::mutex lock_;

  Client &RemoteDataClient(int worker_id) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = clients_.find(worker_id);
    if (found != clients_.end()) return found->second;
    return clients_
        .emplace(
            std::piecewise_construct, std::forward_as_tuple(worker_id),
            std::forward_as_tuple(system_, coordination_.GetEndpoint(worker_id),
                                  kRemoteDataRpcName))
        .first->second;
  }
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
