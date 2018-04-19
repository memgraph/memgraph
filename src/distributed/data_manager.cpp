#include "distributed/data_manager.hpp"
#include "database/storage.hpp"

namespace distributed {

template <typename TRecord>
Cache<TRecord> &DataManager::GetCache(CacheT<TRecord> &collection,
                                      tx::TransactionId tx_id) {
  auto access = collection.access();
  auto found = access.find(tx_id);
  if (found != access.end()) return found->second;

  return access
      .emplace(tx_id, std::make_tuple(tx_id),
               std::make_tuple(std::ref(storage_), std::ref(data_clients_)))
      .first->second;
}

template <>
Cache<Vertex> &DataManager::Elements<Vertex>(tx::TransactionId tx_id) {
  return GetCache(vertices_caches_, tx_id);
}

template <>
Cache<Edge> &DataManager::Elements<Edge>(tx::TransactionId tx_id) {
  return GetCache(edges_caches_, tx_id);
}

DataManager::DataManager(database::Storage &storage,
                         distributed::DataRpcClients &data_clients)
    : storage_(storage), data_clients_(data_clients) {}

void DataManager::ClearCacheForSingleTransaction(tx::TransactionId tx_id) {
  Elements<Vertex>(tx_id).ClearCache();
  Elements<Edge>(tx_id).ClearCache();
}

void DataManager::ClearTransactionalCache(tx::TransactionId oldest_active) {
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
