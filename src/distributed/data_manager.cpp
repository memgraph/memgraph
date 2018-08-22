#include "distributed/data_manager.hpp"
#include "database/storage.hpp"

namespace {

template <typename TCache>
void ClearCache(TCache &cache, tx::TransactionId tx_id) {
  auto access = cache.access();
  auto found = access.find(tx_id);
  if (found != access.end()) found->second.clear();
}

template <typename TCache>
void DeleteOld(TCache &cache, tx::TransactionId oldest_active) {
  auto access = cache.access();
  for (auto &kv : access) {
    if (kv.first < oldest_active) {
      access.remove(kv.first);
    }
  }
}

}  // anonymous namespace

namespace distributed {

template <>
DataManager::CacheT<Vertex> &DataManager::caches<Vertex>() {
  return vertices_caches_;
}

template <>
DataManager::CacheT<Edge> &DataManager::caches<Edge>() {
  return edges_caches_;
}

DataManager::DataManager(database::GraphDb &db,
                         distributed::DataRpcClients &data_clients)
    : db_(db), data_clients_(data_clients) {}

std::mutex &DataManager::GetLock(tx::TransactionId tx_id) {
  auto accessor = lock_store_.access();
  auto found = accessor.find(tx_id);
  if (found != accessor.end()) return found->second;

  // By passing empty tuple default constructor is used
  // and std::mutex is created in ConcurrentMap.
  return accessor.emplace(tx_id, std::make_tuple(tx_id), std::make_tuple())
      .first->second;
}

template <>
void DataManager::LocalizeAddresses<Vertex>(Vertex &vertex) {
  auto localize_edges = [this](auto &edges) {
    for (auto &element : edges) {
      element.vertex = db_.storage().LocalizedAddressIfPossible(element.vertex);
      element.edge = db_.storage().LocalizedAddressIfPossible(element.edge);
    }
  };

  localize_edges(vertex.in_.storage());
  localize_edges(vertex.out_.storage());
}

template <>
void DataManager::LocalizeAddresses(Edge &edge) {
  edge.from_ = db_.storage().LocalizedAddressIfPossible(edge.from_);
  edge.to_ = db_.storage().LocalizedAddressIfPossible(edge.to_);
}

void DataManager::ClearCacheForSingleTransaction(tx::TransactionId tx_id) {
  ClearCache(vertices_caches_, tx_id);
  ClearCache(edges_caches_, tx_id);
}

void DataManager::ClearTransactionalCache(tx::TransactionId oldest_active) {
  DeleteOld(vertices_caches_, oldest_active);
  DeleteOld(edges_caches_, oldest_active);
  DeleteOld(lock_store_, oldest_active);
}

}  // namespace distributed

