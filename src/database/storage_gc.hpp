#pragma once

#include <chrono>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/storage.hpp"
#include "mvcc/version_list.hpp"
#include "stats/metrics.hpp"
#include "storage/deferred_deleter.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/gid.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"
#include "utils/scheduler.hpp"

namespace database {

namespace {

stats::Gauge &gc_running = stats::GetGauge("storage.garbage_collection", 0);

}  // namespace

/** Garbage collection capabilities for database::Storage. Extracted into a
 * separate class for better code organization, and because the GC requires a
 * tx::Engine, while the Storage itself can exist without it. Even though, a
 * database::Storage is always acompanied by a Gc.
 */
class StorageGc {
  template <typename TRecord>
  class MvccDeleter {
    using VlistT = mvcc::VersionList<TRecord>;

   public:
    explicit MvccDeleter(ConcurrentMap<gid::Gid, VlistT *> &collection)
        : gc_(collection, record_deleter_, version_list_deleter_) {}
    DeferredDeleter<TRecord> record_deleter_;
    DeferredDeleter<mvcc::VersionList<TRecord>> version_list_deleter_;
    GarbageCollector<ConcurrentMap<gid::Gid, VlistT *>, TRecord> gc_;
  };

 public:
  /** Creates a garbage collector for the given storage that uses the given
   * tx::Engine. If `pause_sec` is greater then zero, then GC gets triggered
   * periodically. */
  StorageGc(Storage &storage, tx::Engine &tx_engine, int pause_sec)
      : storage_(storage),
        tx_engine_(tx_engine),
        vertices_(storage.vertices_),
        edges_(storage.edges_) {
    if (pause_sec > 0)
      scheduler_.Run(std::chrono::seconds(pause_sec),
                     [this] { CollectGarbage(); });
  }

  ~StorageGc() {
    scheduler_.Stop();

    edges_.record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
    vertices_.record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
    edges_.version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
    vertices_.version_list_deleter_.FreeExpiredObjects(
        tx::Transaction::MaxId());
  }

  StorageGc(const StorageGc &) = delete;
  StorageGc(StorageGc &&) = delete;
  StorageGc &operator=(const StorageGc &) = delete;
  StorageGc &operator=(StorageGc &&) = delete;

  void CollectGarbage() {
    // main garbage collection logic
    // see wiki documentation for logic explanation
    LOG(INFO) << "Garbage collector started";
    const auto snapshot = tx_engine_.GlobalGcSnapshot();
    {
      // This can be run concurrently
      utils::Timer x;

      gc_running.Set(1);
      vertices_.gc_.Run(snapshot, tx_engine_);
      edges_.gc_.Run(snapshot, tx_engine_);
      gc_running.Set(0);

      VLOG(1) << "Garbage collector mvcc phase time: " << x.Elapsed().count();
    }
    // This has to be run sequentially after gc because gc modifies
    // version_lists and changes the oldest visible record, on which Refresh
    // depends.
    {
      // This can be run concurrently
      utils::Timer x;
      storage_.labels_index_.Refresh(snapshot, tx_engine_);
      storage_.label_property_index_.Refresh(snapshot, tx_engine_);
      VLOG(1) << "Garbage collector index phase time: " << x.Elapsed().count();
    }
    {
      // We free expired objects with snapshot.back(), which is
      // the ID of the oldest active transaction (or next active, if there
      // are no currently active). That's legal because that was the
      // last possible transaction that could have obtained pointers
      // to those records. New snapshot can be used, different than one used for
      // first two phases of gc.
      utils::Timer x;
      const auto snapshot = tx_engine_.GlobalGcSnapshot();
      edges_.record_deleter_.FreeExpiredObjects(snapshot.back());
      vertices_.record_deleter_.FreeExpiredObjects(snapshot.back());
      edges_.version_list_deleter_.FreeExpiredObjects(snapshot.back());
      vertices_.version_list_deleter_.FreeExpiredObjects(snapshot.back());
      VLOG(1) << "Garbage collector deferred deletion phase time: "
              << x.Elapsed().count();
    }

    LOG(INFO) << "Garbage collector finished";
    VLOG(2) << "gc snapshot: " << snapshot;
    VLOG(2) << "edge_record_deleter_ size: " << edges_.record_deleter_.Count();
    VLOG(2) << "vertex record deleter_ size: "
            << vertices_.record_deleter_.Count();
    VLOG(2) << "edge_version_list_deleter_ size: "
            << edges_.version_list_deleter_.Count();
    VLOG(2) << "vertex_version_list_deleter_ size: "
            << vertices_.version_list_deleter_.Count();
    VLOG(2) << "vertices_ size: " << storage_.vertices_.access().size();
    VLOG(2) << "edges_ size: " << storage_.edges_.access().size();
  }

 private:
  Storage &storage_;
  tx::Engine &tx_engine_;
  MvccDeleter<Vertex> vertices_;
  MvccDeleter<Edge> edges_;
  Scheduler scheduler_;
};
}  // namespace database
