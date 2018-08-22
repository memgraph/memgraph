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
  using CacheG =
      Cache<gid::Gid,
            std::pair<std::unique_ptr<TRecord>, std::unique_ptr<TRecord>>>;

  template <typename TRecord>
  using CacheT = ConcurrentMap<tx::TransactionId, CacheG<TRecord>>;

 public:
  DataManager(database::GraphDb &db, distributed::DataRpcClients &data_clients);

  /// Returns the new data for the given ID. Creates it (as copy of old) if
  /// necessary.
  template <typename TRecord>
  TRecord *FindNew(tx::TransactionId tx_id, gid::Gid gid) {
    auto &cache = GetCache<TRecord>(tx_id);

    std::lock_guard<std::mutex> guard(GetLock(tx_id));
    auto found = cache.find(gid);
    DCHECK(found != cache.end())
        << "FindNew is called on uninitialized remote Vertex/Edge";

    auto &pair = found->second;
    if (!pair.second) {
      pair.second = std::unique_ptr<TRecord>(pair.first->CloneData());
    }

    return pair.second.get();
  }

  /// For the Vertex/Edge with the given global ID, looks for the data visible
  /// from the given transaction's ID and command ID, and caches it. Sets the
  /// given pointers to point to the fetched data. Analogue to
  /// mvcc::VersionList::find_set_old_new.
  template <typename TRecord>
  void FindSetOldNew(tx::TransactionId tx_id, int worker_id, gid::Gid gid,
                     TRecord **old_record, TRecord **new_record) {
    auto &cache = GetCache<TRecord>(tx_id);
    auto &lock = GetLock(tx_id);

    {
      std::lock_guard<std::mutex> guard(lock);
      auto found = cache.find(gid);
      if (found != cache.end()) {
        *old_record = found->second.first.get();
        *new_record = found->second.second.get();
        return;
      }
    }

    auto remote = data_clients_.RemoteElement<TRecord>(worker_id, tx_id, gid);
    LocalizeAddresses(*remote);

    // This logic is a bit strange because we need to make sure that someone
    // else didn't get a response and updated the cache before we did and we
    // need a lock for that, but we also need to check if we can now return
    // that result - otherwise we could get incosistent results for remote
    // FindSetOldNew
    std::lock_guard<std::mutex> guard(lock);
    auto it_pair = cache.emplace(std::move(gid),
                                 std::make_pair(std::move(remote), nullptr));

    *old_record = it_pair.first->second.first.get();
    *new_record = it_pair.first->second.second.get();
  }

  /// Sets the given records as (new, old) data for the given gid.
  template <typename TRecord>
  void Emplace(tx::TransactionId tx_id, gid::Gid gid,
               std::unique_ptr<TRecord> old_record,
               std::unique_ptr<TRecord> new_record) {

    if (old_record) LocalizeAddresses(*old_record);
    if (new_record) LocalizeAddresses(*new_record);

    std::lock_guard<std::mutex> guard(GetLock(tx_id));
    // We can't replace existing data because some accessors might be using
    // it.
    // TODO - consider if it's necessary and OK to copy just the data content.
    auto &cache = GetCache<TRecord>(tx_id);
    auto found = cache.find(gid);
    if (found == cache.end())
      cache.emplace(std::move(gid), std::make_pair(std::move(old_record),
                                                   std::move(new_record)));
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
  CacheG<TRecord> &GetCache(tx::TransactionId tx_id) {
    auto accessor = caches<TRecord>().access();
    auto found = accessor.find(tx_id);
    if (found != accessor.end()) return found->second;

    return accessor.emplace(tx_id, std::make_tuple(tx_id), std::make_tuple())
        .first->second;
  }

  std::mutex &GetLock(tx::TransactionId tx_id);

  template <typename TRecord>
  CacheT<TRecord> &caches();

  database::GraphDb &db_;
  DataRpcClients &data_clients_;
  ConcurrentMap<tx::TransactionId, std::mutex> lock_store_;
  CacheT<Vertex> vertices_caches_;
  CacheT<Edge> edges_caches_;
};

}  // namespace distributed
