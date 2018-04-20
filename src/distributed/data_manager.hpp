#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "distributed/cache.hpp"
#include "distributed/data_rpc_clients.hpp"
#include "transactions/type.hpp"

class Vertex;
class Edge;

namespace distributed {

/// Handles remote data caches for edges and vertices, per transaction.
class DataManager {
  template <typename TRecord>
  using CacheT = ConcurrentMap<tx::TransactionId, Cache<TRecord>>;

  // Helper, gets or inserts a data cache for the given transaction.
  template <typename TRecord>
  Cache<TRecord> &GetCache(CacheT<TRecord> &collection,
                           tx::TransactionId tx_id);

 public:
  DataManager(database::GraphDb &db, distributed::DataRpcClients &data_clients);

  /// Gets or creates the remote vertex/edge cache for the given transaction.
  template <typename TRecord>
  Cache<TRecord> &Elements(tx::TransactionId tx_id);

  /// Removes all the caches for a single transaction.
  void ClearCacheForSingleTransaction(tx::TransactionId tx_id);

  /// Clears the cache of local transactions that have expired. The signature of
  /// this method is dictated by `distributed::TransactionalCacheCleaner`.
  void ClearTransactionalCache(tx::TransactionId oldest_active);

 private:
  database::GraphDb &db_;
  DataRpcClients &data_clients_;
  CacheT<Vertex> vertices_caches_;
  CacheT<Edge> edges_caches_;
};

}  // namespace distributed
