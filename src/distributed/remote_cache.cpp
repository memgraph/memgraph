
#include "glog/logging.h"

#include "database/storage.hpp"
#include "distributed/remote_cache.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

template <typename TRecord>
TRecord *RemoteCache<TRecord>::FindNew(gid::Gid gid) {
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

template <typename TRecord>
void RemoteCache<TRecord>::FindSetOldNew(tx::transaction_id_t tx_id,
                                         int worker_id, gid::Gid gid,
                                         TRecord *&old_record,
                                         TRecord *&new_record) {
  {
    std::lock_guard<std::mutex> guard(lock_);
    auto found = cache_.find(gid);
    if (found != cache_.end()) {
      old_record = found->second.first.get();
      new_record = found->second.second.get();
      return;
    }
  }

  auto remote =
      remote_data_clients_.RemoteElement<TRecord>(worker_id, tx_id, gid);
  LocalizeAddresses(*remote);

  // This logic is a bit strange because we need to make sure that someone
  // else didn't get a response and updated the cache before we did and we
  // need a lock for that, but we also need to check if we can now return
  // that result - otherwise we could get incosistent results for remote
  // FindSetOldNew
  std::lock_guard<std::mutex> guard(lock_);
  auto it_pair = cache_.emplace(
      gid, std::make_pair<rec_uptr, rec_uptr>(std::move(remote), nullptr));

  old_record = it_pair.first->second.first.get();
  new_record = it_pair.first->second.second.get();
}

template <typename TRecord>
void RemoteCache<TRecord>::emplace(gid::Gid gid, rec_uptr old_record,
                                   rec_uptr new_record) {
  if (old_record) LocalizeAddresses(*old_record);
  if (new_record) LocalizeAddresses(*new_record);

  std::lock_guard<std::mutex> guard{lock_};
  // We can't replace existing data because some accessors might be using
  // it.
  // TODO - consider if it's necessary and OK to copy just the data content.
  auto found = cache_.find(gid);
  if (found != cache_.end())
    return;
  else
    cache_[gid] = std::make_pair(std::move(old_record), std::move(new_record));
}

template <typename TRecord>
void RemoteCache<TRecord>::ClearCache() {
  std::lock_guard<std::mutex> guard{lock_};
  cache_.clear();
}

template <>
void RemoteCache<Vertex>::LocalizeAddresses(Vertex &vertex) {
  auto localize_edges = [this](auto &edges) {
    for (auto &element : edges) {
      element.vertex = storage_.LocalizedAddressIfPossible(element.vertex);
      element.edge = storage_.LocalizedAddressIfPossible(element.edge);
    }
  };

  localize_edges(vertex.in_.storage());
  localize_edges(vertex.out_.storage());
}

template <>
void RemoteCache<Edge>::LocalizeAddresses(Edge &edge) {
  edge.from_ = storage_.LocalizedAddressIfPossible(edge.from_);
  edge.to_ = storage_.LocalizedAddressIfPossible(edge.to_);
}

template class RemoteCache<Vertex>;
template class RemoteCache<Edge>;

}  // namespace distributed
