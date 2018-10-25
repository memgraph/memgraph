#pragma once

#include <glog/logging.h>

#include "data_structures/concurrent/skiplist.hpp"
#include "mvcc/single_node/version_list.hpp"
#include "storage/single_node/deferred_deleter.hpp"
#include "transactions/single_node/engine.hpp"

/**
 * @brief - Garbage collects deleted records.
 * @tparam TCollection - type of collection. Must have a SkipList-like API
 * (accessors).
 * @tparam TRecord - type of underlying record in mvcc.
 */
template <typename TCollection, typename TRecord>
class GarbageCollector {
 public:
  GarbageCollector(
      TCollection &collection, DeferredDeleter<TRecord> &record_deleter,
      DeferredDeleter<mvcc::VersionList<TRecord>> &version_list_deleter)
      : collection_(collection),
        record_deleter_(record_deleter),
        version_list_deleter_(version_list_deleter) {}

  /**
   * @brief - Runs garbage collector. Populates deferred deleters with version
   * lists and records.
   *
   * @param snapshot - the GC snapshot. Consists of the oldest active
   * transaction's snapshot, with that transaction's id appened as last.
   * @param engine - reference to engine object
   */
  void Run(const tx::Snapshot &snapshot, const tx::Engine &engine) {
    auto collection_accessor = collection_.access();
    uint64_t count = 0;
    for (auto id_vlist : collection_accessor) {
      mvcc::VersionList<TRecord> *vlist = id_vlist.second;
      // If the version_list is empty, i.e. there is nothing else to be read
      // from it we can delete it.
      auto ret = vlist->GcDeleted(snapshot, engine);
      if (ret.first) {
        version_list_deleter_.AddObject(vlist, engine.LocalLast());
        count += collection_accessor.remove(id_vlist.first);
      }
      if (ret.second != nullptr) {
        record_deleter_.AddObject(ret.second, engine.LocalLast());
      }
    }
    DLOG_IF(INFO, count > 0)
        << "GC started cleaning with snapshot: " << snapshot;
    DLOG_IF(INFO, count > 0) << "Destroyed: " << count;
  }

 private:
  TCollection &collection_;
  DeferredDeleter<TRecord> &record_deleter_;
  DeferredDeleter<mvcc::VersionList<TRecord>> &version_list_deleter_;
};
