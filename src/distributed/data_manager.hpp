/// @file

#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/distributed/graph_db.hpp"
#include "distributed/data_rpc_clients.hpp"
#include "transactions/type.hpp"
#include "utils/cache.hpp"

class Vertex;
class Edge;

namespace distributed {

/// A wrapper for cached vertex/edge from other machines in the distributed
/// system.
///
/// @tparam TRecord Vertex or Edge
template <typename TRecord>
struct CachedRecordData {
  CachedRecordData(int64_t cypher_id, std::unique_ptr<TRecord> old_record,
                   std::unique_ptr<TRecord> new_record)
      : cypher_id(cypher_id),
        old_record(std::move(old_record)),
        new_record(std::move(new_record)) {}
  int64_t cypher_id;
  std::unique_ptr<TRecord> old_record;
  std::unique_ptr<TRecord> new_record;
};

/// Handles remote data caches for edges and vertices, per transaction.
class DataManager {
  template <typename TRecord>
  using CacheG = utils::Cache<gid::Gid, CachedRecordData<TRecord>>;

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

    auto &data = found->second;
    if (!data.new_record) {
      data.new_record = std::unique_ptr<TRecord>(data.old_record->CloneData());
    }
    return data.new_record.get();
  }

  /// For the Vertex/Edge with the given global ID, looks for the data visible
  /// from the given transaction's ID and command ID, and caches it. Sets the
  /// given pointers to point to the fetched data. Analogue to
  /// mvcc::VersionList::find_set_old_new.
  // TODO (vkasljevic) remove this and use Find instead
  template <typename TRecord>
  void FindSetOldNew(tx::TransactionId tx_id, int worker_id, gid::Gid gid,
                     TRecord **old_record, TRecord **new_record) {
    auto &cache = GetCache<TRecord>(tx_id);
    auto &lock = GetLock(tx_id);

    {
      std::lock_guard<std::mutex> guard(lock);
      auto found = cache.find(gid);
      if (found != cache.end()) {
        *old_record = found->second.old_record.get();
        *new_record = found->second.new_record.get();
        return;
      }
    }

    auto remote = data_clients_.RemoteElement<TRecord>(worker_id, tx_id, gid);
    if (remote.old_record_ptr) LocalizeAddresses(*remote.old_record_ptr);
    if (remote.new_record_ptr) LocalizeAddresses(*remote.new_record_ptr);

    // This logic is a bit strange because we need to make sure that someone
    // else didn't get a response and updated the cache before we did and we
    // need a lock for that, but we also need to check if we can now return
    // that result - otherwise we could get incosistent results for remote
    // FindSetOldNew
    std::lock_guard<std::mutex> guard(lock);
    auto it_pair = cache.emplace(
        std::move(gid), CachedRecordData<TRecord>(
                            remote.cypher_id, std::move(remote.old_record_ptr),
                            std::move(remote.new_record_ptr)));

    *old_record = it_pair.first->second.old_record.get();
    *new_record = it_pair.first->second.new_record.get();
  }

  /// Finds cached element for the given transaction, worker and gid.
  ///
  /// @tparam TRecord Vertex or Edge
  template <typename TRecord>
  const CachedRecordData<TRecord> &Find(tx::TransactionId tx_id, int worker_id,
                                        gid::Gid gid) {
    auto &cache = GetCache<TRecord>(tx_id);
    std::unique_lock<std::mutex> guard(GetLock(tx_id));
    auto found = cache.find(gid);
    if (found != cache.end()) {
      return found->second;
    } else {
      guard.unlock();
      auto remote = data_clients_.RemoteElement<TRecord>(worker_id, tx_id, gid);
      if (remote.old_record_ptr) LocalizeAddresses(*remote.old_record_ptr);
      if (remote.new_record_ptr) LocalizeAddresses(*remote.new_record_ptr);
      guard.lock();
      return cache
          .emplace(std::move(gid),
                   CachedRecordData<TRecord>(remote.cypher_id,
                                             std::move(remote.old_record_ptr),
                                             std::move(remote.new_record_ptr)))
          .first->second;
    }
  }

  /// Sets the given records as (new, old) data for the given gid.
  template <typename TRecord>
  void Emplace(tx::TransactionId tx_id, gid::Gid gid,
               CachedRecordData<TRecord> data) {
    if (data.old_record) LocalizeAddresses(*data.old_record);
    if (data.new_record) LocalizeAddresses(*data.new_record);

    std::lock_guard<std::mutex> guard(GetLock(tx_id));
    // We can't replace existing data because some accessors might be using
    // it.
    // TODO - consider if it's necessary and OK to copy just the data content.
    auto &cache = GetCache<TRecord>(tx_id);
    auto found = cache.find(gid);
    if (found == cache.end()) cache.emplace(std::move(gid), std::move(data));
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
