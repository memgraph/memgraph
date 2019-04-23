/// @file
#pragma once

#include <chrono>
#include <queue>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/distributed/deferred_deleter.hpp"
#include "storage/distributed/edge.hpp"
#include "storage/distributed/garbage_collector.hpp"
#include "storage/distributed/gid.hpp"
#include "storage/distributed/mvcc/version_list.hpp"
#include "storage/distributed/storage.hpp"
#include "storage/distributed/vertex.hpp"
#include "transactions/distributed/engine.hpp"
#include "utils/exceptions.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"

namespace database {

/**
 * Garbage collection capabilities for database::Storage. Extracted into a
 * separate class for better code organization, and because the GC requires a
 * tx::Engine, while the Storage itself can exist without it. Even though, a
 * database::Storage is always acompanied by a Gc.
 */
class StorageGc final {
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
  /**
   * Creates a garbage collector for the given storage that uses the given
   * tx::Engine. If `pause_sec` is greater then zero, then GC gets triggered
   * periodically. The `collect_commit_log_garbage` callback is used as a
   * abstraction for the garbage collector that knows how to handle commit log
   * garbage collection on master or worker implementation.
   *
   * @param storage - pointer to the current storage instance
   * @param tx_engine - pointer to the current transaction engine instance
   * @param pause_sec - garbage collector interval in seconds
   * @param collect_commit_log_garbage - callback for the garbage collector
   *                                     that handles certain implementation
   */
  StorageGc(Storage *storage, tx::Engine *tx_engine, int pause_sec,
            std::function<void(tx::TransactionId, tx::Engine *)>
                collect_commit_log_garbage)
      : storage_(storage),
        tx_engine_(tx_engine),
        vertices_(storage->vertices_),
        edges_(storage->edges_),
        collect_commit_log_garbage_(collect_commit_log_garbage) {
    if (pause_sec > 0)
      scheduler_.Run(
          "Storage GC", std::chrono::seconds(pause_sec), [this] {
            try {
              CollectGarbage();
            } catch (const utils::BasicException &e) {
              DLOG(WARNING)
                  << "Couldn't perform storage garbage collection due to: "
                  << e.what();
            }
          });
  }

  ~StorageGc() {
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
    VLOG(21) << "Garbage collector started";
    const auto snapshot_gc = tx_engine_->GlobalGcSnapshot();
    {
      // This can be run concurrently
      utils::Timer x;

      vertices_.gc_.Run(snapshot_gc, *tx_engine_);
      edges_.gc_.Run(snapshot_gc, *tx_engine_);

      VLOG(21) << "Garbage collector mvcc phase time: " << x.Elapsed().count();
    }
    // This has to be run sequentially after gc because gc modifies
    // version_lists and changes the oldest visible record, on which Refresh
    // depends.
    {
      // This can be run concurrently
      utils::Timer x;
      storage_->labels_index_.Refresh(snapshot_gc, *tx_engine_);
      storage_->label_property_index_.Refresh(snapshot_gc, *tx_engine_);
      VLOG(21) << "Garbage collector index phase time: " << x.Elapsed().count();
    }
    {
      // We free expired objects with snapshot.back(), which is
      // the ID of the oldest active transaction (or next active, if there
      // are no currently active). That's legal because that was the
      // last possible transaction that could have obtained pointers
      // to those records. New snapshot can be used, different than one used for
      // first two phases of gc.
      utils::Timer x;
      const auto snapshot_gc = tx_engine_->GlobalGcSnapshot();
      edges_.record_deleter_.FreeExpiredObjects(snapshot_gc.back());
      vertices_.record_deleter_.FreeExpiredObjects(snapshot_gc.back());
      edges_.version_list_deleter_.FreeExpiredObjects(snapshot_gc.back());
      vertices_.version_list_deleter_.FreeExpiredObjects(snapshot_gc.back());
      VLOG(21) << "Garbage collector deferred deletion phase time: "
               << x.Elapsed().count();
    }

    collect_commit_log_garbage_(snapshot_gc.back(), tx_engine_);
    gc_txid_ranges_.emplace(snapshot_gc.back(), tx_engine_->GlobalLast());

    VLOG(21) << "gc snapshot: " << snapshot_gc;
    VLOG(21) << "edge_record_deleter_ size: " << edges_.record_deleter_.Count();
    VLOG(21) << "vertex record deleter_ size: "
             << vertices_.record_deleter_.Count();
    VLOG(21) << "edge_version_list_deleter_ size: "
             << edges_.version_list_deleter_.Count();
    VLOG(21) << "vertex_version_list_deleter_ size: "
             << vertices_.version_list_deleter_.Count();
    VLOG(21) << "vertices_ size: " << storage_->vertices_.access().size();
    VLOG(21) << "edges_ size: " << storage_->edges_.access().size();
    VLOG(21) << "Garbage collector finished.";
  }

  /**
   * Find the largest transaction from which everything older is safe to
   * delete, ones for which the hints have been set in the gc phase, and no
   * alive transaction from the time before the hints were set is still alive
   * (otherwise that transaction could still be waiting for a resolution of
   * the query to the commit log about some old transaction)
   */
  std::optional<tx::TransactionId> GetClogSafeTransaction(
      tx::TransactionId oldest_active) {
    std::optional<tx::TransactionId> safe_to_delete;
    while (!gc_txid_ranges_.empty() &&
           gc_txid_ranges_.front().second < oldest_active) {
      safe_to_delete = gc_txid_ranges_.front().first;
      gc_txid_ranges_.pop();
    }
    return safe_to_delete;
  }

  bool IsRunning() { return scheduler_.IsRunning(); }

  void Stop() { scheduler_.Stop(); }

 private:
  Storage *storage_;
  tx::Engine *tx_engine_;
  MvccDeleter<Vertex> vertices_;
  MvccDeleter<Edge> edges_;
  utils::Scheduler scheduler_;

  // History of <oldest active transaction, next transaction to be ran> ranges
  // that gc operated on at some previous time - used to clear commit log
  std::queue<std::pair<tx::TransactionId, tx::TransactionId>> gc_txid_ranges_;

  std::function<void(tx::TransactionId, tx::Engine *)>
      collect_commit_log_garbage_;
};
}  // namespace database
