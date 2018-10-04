#pragma once

#include <glog/logging.h>

#include "data_structures/concurrent/skiplist.hpp"
#include "mvcc/distributed/version_list.hpp"
#include "storage/distributed/deferred_deleter.hpp"
#include "transactions/engine.hpp"

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
    std::vector<typename DeferredDeleter<TRecord>::DeletedObject>
        deleted_records;
    std::vector<
        typename DeferredDeleter<mvcc::VersionList<TRecord>>::DeletedObject>
        deleted_version_lists;
    for (auto id_vlist : collection_accessor) {
      mvcc::VersionList<TRecord> *vlist = id_vlist.second;
      // If the version_list is empty, i.e. there is nothing else to be read
      // from it we can delete it.
      auto ret = vlist->GcDeleted(snapshot, engine);
      if (ret.first) {
        deleted_version_lists.emplace_back(vlist, engine.LocalLast());
        count += collection_accessor.remove(id_vlist.first);
      }
      if (ret.second != nullptr)
        deleted_records.emplace_back(ret.second, engine.LocalLast());
    }
    DLOG_IF(INFO, count > 0)
        << "GC started cleaning with snapshot: " << snapshot;
    DLOG_IF(INFO, count > 0) << "Destroyed: " << count;

    // Add records to deleter, with the id larger or equal than the last active
    // transaction.
    record_deleter_.AddObjects(deleted_records);
    // Add version_lists to deleter, with the id larger or equal than the last
    // active transaction.
    version_list_deleter_.AddObjects(deleted_version_lists);
  }

 private:
  TCollection &collection_;
  DeferredDeleter<TRecord> &record_deleter_;
  DeferredDeleter<mvcc::VersionList<TRecord>> &version_list_deleter_;
};
