#include "distributed/remote_data_manager.hpp"
#include "database/storage.hpp"

namespace distributed {

template <typename TRecord>
RemoteCache<TRecord> &RemoteDataManager::GetCache(CacheT<TRecord> &collection,
                                                  tx::transaction_id_t tx_id) {
  auto access = collection.access();
  auto found = access.find(tx_id);
  if (found != access.end()) return found->second;

  return access
      .emplace(
          tx_id, std::make_tuple(tx_id),
          std::make_tuple(std::ref(storage_), std::ref(remote_data_clients_)))
      .first->second;
}

template <>
RemoteCache<Vertex> &RemoteDataManager::Elements<Vertex>(
    tx::transaction_id_t tx_id) {
  return GetCache(vertices_caches_, tx_id);
}

template <>
RemoteCache<Edge> &RemoteDataManager::Elements<Edge>(
    tx::transaction_id_t tx_id) {
  return GetCache(edges_caches_, tx_id);
}

RemoteDataManager::RemoteDataManager(
    database::Storage &storage,
    distributed::RemoteDataRpcClients &remote_data_clients)
    : storage_(storage), remote_data_clients_(remote_data_clients) {}

void RemoteDataManager::ClearCacheForSingleTransaction(
    tx::transaction_id_t tx_id) {
  Elements<Vertex>(tx_id).ClearCache();
  Elements<Edge>(tx_id).ClearCache();
}

void RemoteDataManager::ClearTransactionalCache(
    tx::transaction_id_t oldest_active) {
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

}  // namespace distributed
