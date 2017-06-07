#include <functional>

#include "gflags/gflags.h"

#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/recovery.hpp"
#include "logging/logger.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"

DEFINE_int32(GC_CYCLE_SEC, 30,
             "Amount of time between starts of two cleaning cycles in seconds. "
             "-1 to turn off.");
DEFINE_int32(MAX_RETAINED_SNAPSHOTS, -1,
             "Number of retained snapshots, -1 means without limit.");
DEFINE_int32(SNAPSHOT_CYCLE_SEC, -1,
             "Amount of time between starts of two snapshooters in seconds. -1 "
             "to turn off.");
DEFINE_bool(SNAPSHOT_ON_DB_DESTRUCTION, false,
            "Snapshot on database destruction.");

DECLARE_string(SNAPSHOT_DIRECTORY);

GraphDb::GraphDb(const std::string &name, const fs::path &snapshot_db_dir)
    : name_(name),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_) {
  // Pause of -1 means we shouldn't run the GC.
  if (FLAGS_GC_CYCLE_SEC != -1) {
    gc_scheduler_.Run(std::chrono::seconds(FLAGS_GC_CYCLE_SEC), [this]() {
      // main garbage collection logic
      // see wiki documentation for logic explanation
      const auto next_id = this->tx_engine.count() + 1;
      const auto id = this->tx_engine.oldest_active().get_or(next_id);
      {
        // This can be run concurrently
        this->gc_vertices_.Run(id, this->tx_engine);
        this->gc_edges_.Run(id, this->tx_engine);
      }
      // This has to be run sequentially after gc because gc modifies
      // version_lists and changes the oldest visible record, on which Refresh
      // depends.
      {
        // This can be run concurrently
        this->labels_index_.Refresh(id, this->tx_engine);
        this->edge_types_index_.Refresh(id, this->tx_engine);
      }
      this->edge_record_deleter_.FreeExpiredObjects(id);
      this->vertex_record_deleter_.FreeExpiredObjects(id);
      this->edge_version_list_deleter_.FreeExpiredObjects(id);
      this->vertex_version_list_deleter_.FreeExpiredObjects(id);
    });
  }

  RecoverDatabase(snapshot_db_dir);
  StartSnapshooting();
}

void GraphDb::StartSnapshooting() {
  if (FLAGS_SNAPSHOT_CYCLE_SEC != -1) {
    auto create_snapshot = [this]() -> void {
      GraphDbAccessor db_accessor(*this);
      snapshooter_.MakeSnapshot(db_accessor,
                                fs::path(FLAGS_SNAPSHOT_DIRECTORY) / name_,
                                FLAGS_MAX_RETAINED_SNAPSHOTS);
    };
    snapshot_creator_.Run(std::chrono::seconds(FLAGS_SNAPSHOT_CYCLE_SEC),
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
    if (recovery.Recover(snapshot_file.string(), db_accessor)) {
      return;
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
  if (FLAGS_SNAPSHOT_ON_DB_DESTRUCTION == true) {
    GraphDbAccessor db_accessor(*this);
    snapshooter_.MakeSnapshot(db_accessor,
                              fs::path(FLAGS_SNAPSHOT_DIRECTORY) / name_,
                              FLAGS_MAX_RETAINED_SNAPSHOTS);
  }

  // Delete vertices and edges which weren't collected before, also deletes
  // records inside version list
  for (auto &vertex : this->vertices_.access()) delete vertex;
  for (auto &edge : this->edges_.access()) delete edge;

  // Free expired records with the maximal possible id from all the deleters.
  this->edge_record_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->vertex_record_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->edge_version_list_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->vertex_version_list_deleter_.FreeExpiredObjects(Id::MaximalId());
}
