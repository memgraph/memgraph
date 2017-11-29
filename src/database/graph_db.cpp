#include <experimental/filesystem>
#include <functional>

#include <glog/logging.h>

#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "transactions/engine_master.hpp"
#include "utils/timer.hpp"

namespace fs = std::experimental::filesystem;

GraphDb::GraphDb(GraphDb::Config config)
    : config_(config),
      tx_engine_(new tx::MasterEngine()),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_),
      wal_{config.durability_directory, config.durability_enabled} {
  // Pause of -1 means we shouldn't run the GC.
  if (config.gc_cycle_sec != -1) {
    gc_scheduler_.Run(std::chrono::seconds(config.gc_cycle_sec),
                      [this]() { CollectGarbage(); });
  }

  // If snapshots are enabled we need the durability dir.
  if (config.durability_enabled)
    durability::CheckDurabilityDir(config.durability_directory);

  if (config.db_recover_on_startup)
    durability::Recover(config.durability_directory, *this);
  if (config.durability_enabled) wal_.Enable();
  StartSnapshooting();

  if (config.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        std::chrono::seconds(
            std::max(1, std::min(5, config.query_execution_time_sec / 4))),
        [this]() {
          tx_engine_->LocalForEachActiveTransaction([this](tx::Transaction &t) {
            if (t.creation_time() +
                    std::chrono::seconds(config_.query_execution_time_sec) <
                std::chrono::steady_clock::now()) {
              t.set_should_abort();
            };
          });
        });
  }
}

void GraphDb::Shutdown() {
  is_accepting_transactions_ = false;
  tx_engine_->LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });
}

void GraphDb::StartSnapshooting() {
  if (config_.durability_enabled) {
    auto create_snapshot = [this]() -> void {
      GraphDbAccessor db_accessor(*this);
      if (!durability::MakeSnapshot(db_accessor,
                                    fs::path(config_.durability_directory),
                                    config_.snapshot_max_retained)) {
        LOG(WARNING) << "Durability: snapshot creation failed";
      }
      db_accessor.Commit();
    };
    snapshot_creator_.Run(std::chrono::seconds(config_.snapshot_cycle_sec),
                          create_snapshot);
  }
}

void GraphDb::CollectGarbage() {
  // main garbage collection logic
  // see wiki documentation for logic explanation
  LOG(INFO) << "Garbage collector started";
  const auto snapshot = tx_engine_->GlobalGcSnapshot();
  {
    // This can be run concurrently
    utils::Timer x;
    gc_vertices_.Run(snapshot, *tx_engine_);
    gc_edges_.Run(snapshot, *tx_engine_);
    VLOG(1) << "Garbage collector mvcc phase time: " << x.Elapsed().count();
  }
  // This has to be run sequentially after gc because gc modifies
  // version_lists and changes the oldest visible record, on which Refresh
  // depends.
  {
    // This can be run concurrently
    utils::Timer x;
    labels_index_.Refresh(snapshot, *tx_engine_);
    label_property_index_.Refresh(snapshot, *tx_engine_);
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
    const auto snapshot = tx_engine_->GlobalGcSnapshot();
    edge_record_deleter_.FreeExpiredObjects(snapshot.back());
    vertex_record_deleter_.FreeExpiredObjects(snapshot.back());
    edge_version_list_deleter_.FreeExpiredObjects(snapshot.back());
    vertex_version_list_deleter_.FreeExpiredObjects(snapshot.back());
    VLOG(1) << "Garbage collector deferred deletion phase time: "
            << x.Elapsed().count();
  }

  LOG(INFO) << "Garbage collector finished";
  VLOG(2) << "gc snapshot: " << snapshot;
  VLOG(2) << "edge_record_deleter_ size: " << edge_record_deleter_.Count();
  VLOG(2) << "vertex record deleter_ size: " << vertex_record_deleter_.Count();
  VLOG(2) << "edge_version_list_deleter_ size: "
          << edge_version_list_deleter_.Count();
  VLOG(2) << "vertex_version_list_deleter_ size: "
          << vertex_version_list_deleter_.Count();
  VLOG(2) << "vertices_ size: " << vertices_.access().size();
  VLOG(2) << "edges_ size: " << edges_.access().size();
}

GraphDb::~GraphDb() {
  // Stop the gc scheduler to not run into race conditions for deletions.
  gc_scheduler_.Stop();

  // Stop the snapshot creator to avoid snapshooting while database is beeing
  // deleted.
  snapshot_creator_.Stop();

  // Stop transaction killer.
  transaction_killer_.Stop();

  // Create last database snapshot
  if (config_.snapshot_on_exit == true) {
    GraphDbAccessor db_accessor(*this);
    LOG(INFO) << "Creating snapshot on shutdown..." << std::endl;
    const bool status = durability::MakeSnapshot(
        db_accessor, fs::path(config_.durability_directory),
        config_.snapshot_max_retained);
    if (status) {
      std::cout << "Snapshot created successfully." << std::endl;
    } else {
      LOG(ERROR) << "Snapshot creation failed!" << std::endl;
    }
  }

  // Delete vertices and edges which weren't collected before, also deletes
  // records inside version list
  for (auto &id_vlist : vertices_.access()) delete id_vlist.second;
  for (auto &id_vlist : edges_.access()) delete id_vlist.second;

  // Free expired records with the maximal possible id from all the deleters.
  edge_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  vertex_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  edge_version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  vertex_version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
}
