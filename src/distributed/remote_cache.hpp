#pragma once

#include <mutex>
#include <unordered_map>

#include "distributed/remote_data_rpc_clients.hpp"
#include "storage/gid.hpp"

namespace database {
class Storage;
}

namespace distributed {

/**
 * Used for caching Vertices and Edges that are stored on another worker in a
 * distributed system. Maps global IDs to (old, new) Vertex/Edge pointer
 * pairs.  It is possible that either "old" or "new" are nullptrs, but at
 * least one must be not-null. The RemoteCache is the owner of TRecord
 * objects it points to.
 *
 * @tparam TRecord - Edge or Vertex
 */
template <typename TRecord>
class RemoteCache {
  using rec_uptr = std::unique_ptr<TRecord>;

 public:
  RemoteCache(database::Storage &storage,
              distributed::RemoteDataRpcClients &remote_data_clients)
      : storage_(storage), remote_data_clients_(remote_data_clients) {}

  /// Returns the new data for the given ID. Creates it (as copy of old) if
  /// necessary.
  TRecord *FindNew(gid::Gid gid);

  /// For the Vertex/Edge with the given global ID, looks for the data visible
  /// from the given transaction's ID and command ID, and caches it. Sets the
  /// given pointers to point to the fetched data. Analogue to
  /// mvcc::VersionList::find_set_old_new.
  void FindSetOldNew(tx::transaction_id_t tx_id, int worker_id, gid::Gid gid,
                     TRecord *&old_record, TRecord *&new_record);

  /// Sets the given records as (new, old) data for the given gid.
  void emplace(gid::Gid gid, rec_uptr old_record, rec_uptr new_record);

  /// Removes all the data from the cache.
  void ClearCache();

 private:
  database::Storage &storage_;

  std::mutex lock_;
  distributed::RemoteDataRpcClients &remote_data_clients_;
  // TODO it'd be better if we had VertexData and EdgeData in here, as opposed
  // to Vertex and Edge.
  std::unordered_map<gid::Gid, std::pair<rec_uptr, rec_uptr>> cache_;

  // Localizes all the addresses in the record.
  void LocalizeAddresses(TRecord &record);
};

}  // namespace distributed
