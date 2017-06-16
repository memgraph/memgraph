#include <functional>

#include "gflags/gflags.h"

#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/recovery.hpp"
#include "logging/logger.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"

DEFINE_int32(gc_cycle_sec, 30,
             "Amount of time between starts of two cleaning cycles in seconds. "
             "-1 to turn off.");
DEFINE_int32(max_retained_snapshots, -1,
             "Number of retained snapshots, -1 means without limit.");
DEFINE_int32(snapshot_cycle_sec, -1,
             "Amount of time between starts of two snapshooters in seconds. -1 "
             "to turn off.");
DEFINE_bool(snapshot_on_db_exit, false,
            "Snapshot on exiting the database.");

DECLARE_string(snapshot_directory);

GraphDb::GraphDb(const std::string &name, const fs::path &snapshot_db_dir)
    : Loggable("GraphDb"),
      name_(name),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_) {
  // Pause of -1 means we shouldn't run the GC.
  if (FLAGS_gc_cycle_sec != -1) {
    gc_scheduler_.Run(std::chrono::seconds(FLAGS_gc_cycle_sec), [this]() {
      // main garbage collection logic
      // see wiki documentation for logic explanation
      const auto snapshot = this->tx_engine.GcSnapshot();
      {
        // This can be run concurrently
        this->gc_vertices_.Run(snapshot, this->tx_engine);
        this->gc_edges_.Run(snapshot, this->tx_engine);
      }
      // This has to be run sequentially after gc because gc modifies
      // version_lists and changes the oldest visible record, on which Refresh
      // depends.
      {
        // This can be run concurrently
        this->labels_index_.Refresh(snapshot, this->tx_engine);
        this->edge_types_index_.Refresh(snapshot, this->tx_engine);
      }
      // we free expired objects with snapshot.back(), which is
      // the ID of the oldest active transaction (or next active, if there
      // are no currently active). that's legal because that was the
      // last possible transaction that could have obtained pointers
      // to those records
      this->edge_record_deleter_.FreeExpiredObjects(snapshot.back());
      this->vertex_record_deleter_.FreeExpiredObjects(snapshot.back());
      this->edge_version_list_deleter_.FreeExpiredObjects(snapshot.back());
      this->vertex_version_list_deleter_.FreeExpiredObjects(snapshot.back());
    });
  }

  RecoverDatabase(snapshot_db_dir);
  StartSnapshooting();
}

void GraphDb::StartSnapshooting() {
  if (FLAGS_snapshot_cycle_sec != -1) {
    auto create_snapshot = [this]() -> void {
      GraphDbAccessor db_accessor(*this);
      snapshooter_.MakeSnapshot(db_accessor,
                                fs::path(FLAGS_snapshot_directory) / name_,
                                FLAGS_max_retained_snapshots);
    };
    snapshot_creator_.Run(std::chrono::seconds(FLAGS_snapshot_cycle_sec),
                          create_snapshot);
  }
}

void GraphDb::RecoverDatabase(const fs::path &snapshot_db_dir) {
  if (snapshot_db_dir.empty()) return;
  std::vector<fs::path> snapshots;
  for (auto &snapshot_file : fs::directory_iterator(snapshot_db_dir))
    snapshots.push_back(snapshot_file);

  std::sort(snapshots.rbegin(), snapshots.rend());
  Recovery recovery;
  for (auto &snapshot_file : snapshots) {
    GraphDbAccessor db_accessor(*this);
    logger.info("Starting database recovery from snapshot {}...",
                snapshot_file);
    if (recovery.Recover(snapshot_file.string(), db_accessor)) {
      logger.info("Recovery successful.");
      return;
    } else {
      logger.error("Recovery unsuccessful, trying older snapshot...");
    }
  }
}

GraphDb::~GraphDb() {
  // Stop the gc scheduler to not run into race conditions for deletions.
  gc_scheduler_.Stop();

  // Stop the snapshot creator to avoid snapshooting while database is beeing
  // deleted.
  snapshot_creator_.Stop();

  // Create last database snapshot
  if (FLAGS_snapshot_on_db_exit == true) {
    GraphDbAccessor db_accessor(*this);
    logger.info("Creating snapshot on shutdown...");
    const bool status = snapshooter_.MakeSnapshot(
        db_accessor, fs::path(FLAGS_snapshot_directory) / name_,
        FLAGS_max_retained_snapshots);
    if (status) {
      logger.info("Snapshot created successfully.");
    } else {
      logger.error("Snapshot creation failed!");
    }
  }

  // Delete vertices and edges which weren't collected before, also deletes
  // records inside version list
  for (auto &vertex : this->vertices_.access()) delete vertex;
  for (auto &edge : this->edges_.access()) delete edge;

  // Free expired records with the maximal possible id from all the deleters.
  this->edge_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  this->vertex_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  this->edge_version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  this->vertex_version_list_deleter_.FreeExpiredObjects(
      tx::Transaction::MaxId());
}
