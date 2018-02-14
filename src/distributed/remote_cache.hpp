#pragma once

#include <mutex>
#include <unordered_map>

#include "glog/logging.h"

#include "distributed/remote_data_rpc_clients.hpp"
#include "storage/gid.hpp"
#include "transactions/transaction.hpp"

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
  RemoteCache(distributed::RemoteDataRpcClients &remote_data_clients)
      : remote_data_clients_(remote_data_clients) {}

  /// Returns the new data for the given ID. Creates it (as copy of old) if
  /// necessary.
  TRecord *FindNew(gid::Gid gid) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = cache_.find(gid);
    DCHECK(found != cache_.end())
        << "FindNew for uninitialized remote Vertex/Edge";
    auto &pair = found->second;
    if (!pair.second) {
      pair.second = std::unique_ptr<TRecord>(pair.first->CloneData());
    }
    return pair.second.get();
  }

  /**
   * For the Vertex/Edge with the given global ID, looks for the data visible
   * from the given transaction's ID and command ID, and caches it. Sets the
   * given pointers to point to the fetched data. Analogue to
   * mvcc::VersionList::find_set_old_new.
   */
  void FindSetOldNew(tx::transaction_id_t tx_id, int worker_id, gid::Gid gid,
                     TRecord *&old_record, TRecord *&new_record) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = cache_.find(gid);
    if (found != cache_.end()) {
      old_record = found->second.first.get();
      new_record = found->second.second.get();
    } else {
      auto remote =
          remote_data_clients_.RemoteElement<TRecord>(worker_id, tx_id, gid);
      old_record = remote.get();
      new_record = nullptr;
      cache_[gid] =
          std::make_pair<rec_uptr, rec_uptr>(std::move(remote), nullptr);
    }
  }

  void AdvanceCommand() {
    // TODO implement.
    // The effect of this should be that the next call to FindSetOldNew will do
    // an RPC and not use the cached stuff.
    //
    // Not sure if it's OK to just flush the cache? I *think* that after a
    // global advance-command, all the existing RecordAccessors will be calling
    // Reconstruct, so perhaps just flushing is the correct sollution, even
    // though we'll have pointers to nothing.
  }

  /** Sets the given records as (new, old) data for the given gid. */
  void emplace(gid::Gid gid, rec_uptr old_record, rec_uptr new_record) {
    std::lock_guard<std::mutex> guard{lock_};
    // We can't replace existing data because some accessors might be using it.
    // TODO - consider if it's necessary and OK to copy just the data content.
    auto found = cache_.find(gid);
    if (found != cache_.end())
      return;
    else
      cache_[gid] =
          std::make_pair(std::move(old_record), std::move(new_record));
  }

  /// Removes all the cached data. All the pointers to that data still held by
  /// RecordAccessors will become invalid and must never be dereferenced after
  /// this call. To make a RecordAccessor valid again Reconstruct must be called
  /// on it. This is typically done after the command advanced.
  void ClearCache() {
    std::lock_guard<std::mutex> guard{lock_};
    cache_.clear();
  }

 private:
  std::mutex lock_;
  distributed::RemoteDataRpcClients &remote_data_clients_;
  // TODO it'd be better if we had VertexData and EdgeData in here, as opposed
  // to Vertex and Edge.
  std::unordered_map<gid::Gid, std::pair<rec_uptr, rec_uptr>> cache_;
};
}  // namespace distributed
