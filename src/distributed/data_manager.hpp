/// @file

#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/distributed/graph_db.hpp"
#include "distributed/cached_record_data.hpp"
#include "distributed/data_rpc_clients.hpp"
#include "transactions/type.hpp"
#include "utils/cache.hpp"

class Vertex;
class Edge;

namespace distributed {
/// Handles remote data caches for edges and vertices, per transaction.
class DataManager {
  template <typename TRecord>
  using CacheG =
      utils::LruCache<gid::Gid, std::shared_ptr<CachedRecordData<TRecord>>>;

  template <typename TRecord>
  using CacheT = ConcurrentMap<tx::TransactionId, CacheG<TRecord>>;

 public:
  DataManager(database::GraphDb &db, distributed::DataRpcClients &data_clients,
              size_t vertex_cache_size, size_t edge_cache_size);

  /// Finds cached element for the given transaction, worker and gid.
  ///
  /// @tparam TRecord Vertex or Edge
  template <typename TRecord>
  std::shared_ptr<CachedRecordData<TRecord>> Find(tx::TransactionId tx_id,
                                                  int from_worker_id,
                                                  int worker_id, gid::Gid gid,
                                                  bool to_update = false) {
    auto &cache = GetCache<TRecord>(tx_id);
    std::unique_lock<std::mutex> guard(GetLock(tx_id));
    auto found = cache.Find(gid);
    if (found) {
      auto data = *found;
      if (to_update && !data->new_record) {
        data->new_record.reset(data->old_record->CloneData());
      }

      return data;
    } else {
      guard.unlock();
      auto remote = data_clients_.RemoteElement<TRecord>(from_worker_id,
                                                         worker_id, tx_id, gid);
      if (remote.old_record_ptr) LocalizeAddresses(*remote.old_record_ptr);
      if (remote.new_record_ptr) LocalizeAddresses(*remote.new_record_ptr);

      if (to_update && !remote.new_record_ptr) {
        remote.new_record_ptr.reset(remote.old_record_ptr->CloneData());
      }

      guard.lock();
      auto data =
          std::make_shared<CachedRecordData<TRecord>>(CachedRecordData<TRecord>{
              remote.cypher_id, std::move(remote.old_record_ptr),
              std::move(remote.new_record_ptr)});
      cache.Insert(gid, data);
      return data;
    }
  }

  /// Sets the given records as (new, old) data for the given gid.
  template <typename TRecord>
  void Emplace(tx::TransactionId tx_id, gid::Gid gid,
               CachedRecordData<TRecord> data) {
    std::lock_guard<std::mutex> guard(GetLock(tx_id));
    // We can't replace existing data because some accessors might be using
    // it.
    // TODO - consider if it's necessary and OK to copy just the data content.
    auto &cache = GetCache<TRecord>(tx_id);
    auto found = cache.Find(gid);
    if (!found) {
      if (data.old_record) LocalizeAddresses(*data.old_record);
      if (data.new_record) LocalizeAddresses(*data.new_record);
      cache.Insert(gid, std::make_shared<CachedRecordData<TRecord>>(std::move(data)));
    }
  }

  /// Removes all the caches for a single transaction.
  void ClearCacheForSingleTransaction(tx::TransactionId tx_id);

  /// Clears the cache of local transactions that have expired. The signature of
  /// this method is dictated by `distributed::TransactionalCacheCleaner`.
  void ClearTransactionalCache(tx::TransactionId oldest_active);

 private:
  template <typename TRecord>
  void LocalizeAddresses(TRecord &record);

  template <typename TRecord>
  size_t GetInitSize() const;

  template <typename TRecord>
  CacheG<TRecord> &GetCache(tx::TransactionId tx_id) {
    auto accessor = caches<TRecord>().access();
    auto found = accessor.find(tx_id);
    if (found != accessor.end()) return found->second;

    return accessor
        .emplace(tx_id, std::make_tuple(tx_id),
                 std::make_tuple(GetInitSize<TRecord>()))
        .first->second;
  }

  std::mutex &GetLock(tx::TransactionId tx_id);

  template <typename TRecord>
  CacheT<TRecord> &caches();

  size_t vertex_cache_size_;
  size_t edge_cache_size_;

  database::GraphDb &db_;
  DataRpcClients &data_clients_;
  ConcurrentMap<tx::TransactionId, std::mutex> lock_store_;
  CacheT<Vertex> vertices_caches_;
  CacheT<Edge> edges_caches_;
};

}  // namespace distributed
