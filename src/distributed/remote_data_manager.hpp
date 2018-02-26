#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/remote_cache.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Handles remote data caches for edges and vertices, per transaction. */
class RemoteDataManager {
  // Helper, gets or inserts a data cache for the given transaction.
  template <typename TCollection>
  auto &GetCache(TCollection &collection, tx::transaction_id_t tx_id) {
    auto access = collection.access();
    auto found = access.find(tx_id);
    if (found != access.end()) return found->second;

    return access
        .emplace(tx_id, std::make_tuple(tx_id),
                 std::make_tuple(std::ref(remote_data_clients_)))
        .first->second;
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

  /// Removes all the caches for a single transaction.
  void ClearCacheForSingleTransaction(tx::transaction_id_t tx_id) {
    Vertices(tx_id).ClearCache();
    Edges(tx_id).ClearCache();
  }

  /// Clears the cache of local transactions that have expired. The signature of
  /// this method is dictated by `distributed::CacheCleaner`.
  void ClearTransactionalCache(tx::transaction_id_t oldest_active) {
    auto vertex_access = vertices_caches_.access();
    for (auto &kv : vertex_access) {
      if (kv.first < oldest_active) {
        vertex_access.remove(kv.first);
      }
    }
    auto edge_access = edges_caches_.access();
    for (auto &kv : edge_access) {
      if (kv.first < oldest_active) {
        edge_access.remove(kv.first);
      }
    }
  }

 private:
  RemoteDataRpcClients &remote_data_clients_;
  ConcurrentMap<tx::transaction_id_t, RemoteCache<Vertex>> vertices_caches_;
  ConcurrentMap<tx::transaction_id_t, RemoteCache<Edge>> edges_caches_;
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
