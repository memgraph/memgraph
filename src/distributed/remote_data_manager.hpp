#pragma once

#include <mutex>

#include "distributed/remote_cache.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Handles remote data caches for edges and vertices, per transaction. */
class RemoteDataManager {
  // Helper, gets or inserts a data cache for the given transaction.
  template <typename TCollection>
  auto &GetCache(TCollection &collection, tx::transaction_id_t tx_id) {
    std::lock_guard<SpinLock> guard{lock_};
    auto found = collection.find(tx_id);
    if (found != collection.end()) return found->second;

    return collection.emplace(tx_id, remote_data_clients_).first->second;
  }

 public:
  RemoteDataManager(distributed::RemoteDataRpcClients &remote_data_clients)
      : remote_data_clients_(remote_data_clients) {}

  /// Gets or creates the remote vertex cache for the given transaction.
  auto &Vertices(tx::transaction_id_t tx_id) {
    return GetCache(vertices_caches_, tx_id);
  }

  /// Gets or creates the remote edge cache for the given transaction.
  auto &Edges(tx::transaction_id_t tx_id) {
    return GetCache(edges_caches_, tx_id);
  }

  /// Gets or creates the remote vertex/edge cache for the given transaction.
  template <typename TRecord>
  auto &Elements(tx::transaction_id_t tx_id);

 private:
  RemoteDataRpcClients &remote_data_clients_;
  SpinLock lock_;
  std::unordered_map<tx::transaction_id_t, RemoteCache<Vertex>>
      vertices_caches_;
  std::unordered_map<tx::transaction_id_t, RemoteCache<Edge>> edges_caches_;
};

template <>
inline auto &RemoteDataManager::Elements<Vertex>(tx::transaction_id_t tx_id) {
  return Vertices(tx_id);
}

template <>
inline auto &RemoteDataManager::Elements<Edge>(tx::transaction_id_t tx_id) {
  return Edges(tx_id);
}
}  // namespace distributed
