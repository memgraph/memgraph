#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/remote_cache.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "transactions/type.hpp"

class Vertex;
class Edge;

namespace database {
class Storage;
}

namespace distributed {

/// Handles remote data caches for edges and vertices, per transaction.
class RemoteDataManager {
  template <typename TRecord>
  using CacheT = ConcurrentMap<tx::transaction_id_t, RemoteCache<TRecord>>;

  // Helper, gets or inserts a data cache for the given transaction.
  template <typename TRecord>
  RemoteCache<TRecord> &GetCache(CacheT<TRecord> &collection,
                                 tx::transaction_id_t tx_id);

 public:
  RemoteDataManager(database::Storage &storage,
                    distributed::RemoteDataRpcClients &remote_data_clients);

  /// Gets or creates the remote vertex/edge cache for the given transaction.
  template <typename TRecord>
  RemoteCache<TRecord> &Elements(tx::transaction_id_t tx_id);

  /// Removes all the caches for a single transaction.
  void ClearCacheForSingleTransaction(tx::transaction_id_t tx_id);

  /// Clears the cache of local transactions that have expired. The signature of
  /// this method is dictated by `distributed::CacheCleaner`.
  void ClearTransactionalCache(tx::transaction_id_t oldest_active);

 private:
  database::Storage &storage_;
  RemoteDataRpcClients &remote_data_clients_;
  CacheT<Vertex> vertices_caches_;
  CacheT<Edge> edges_caches_;
};

}  // namespace distributed
