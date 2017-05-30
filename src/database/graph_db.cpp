#include <functional>

#include "config/config.hpp"
#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "logging/logger.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"

const int DEFAULT_CLEANING_CYCLE_SEC = 30;  // 30 seconds
const std::string DEFAULT_SNAPSHOT_FOLDER = "snapshots";
const int DEFAULT_MAX_RETAINED_SNAPSHOTS = -1;  // unlimited number of snapshots
const int DEFAULT_SNAPSHOT_CYCLE_SEC = -1;      // off

GraphDb::GraphDb(const std::string &name, bool import_snapshot)
    : name_(name),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_) {
  const std::string time_str = CONFIG(config::CLEANING_CYCLE_SEC);
  int pause = DEFAULT_CLEANING_CYCLE_SEC;
  if (!time_str.empty()) pause = CONFIG_INTEGER(config::CLEANING_CYCLE_SEC);
  // Pause of -1 means we shouldn't run the GC.
  if (pause != -1) {
    gc_scheduler_.Run(std::chrono::seconds(pause), [this]() {
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

  // Creating snapshoter
  const std::string max_retained_snapshots_str =
      CONFIG(config::MAX_RETAINED_SNAPSHOTS);
  const std::string snapshot_cycle_sec_str =
      CONFIG(config::MAX_RETAINED_SNAPSHOTS);
  const std::string snapshot_folder_str = CONFIG(config::SNAPSHOTS_PATH);

  int max_retained_snapshots_ = DEFAULT_MAX_RETAINED_SNAPSHOTS;
  if (!max_retained_snapshots_str.empty())
    max_retained_snapshots_ = CONFIG_INTEGER(config::MAX_RETAINED_SNAPSHOTS);

  int snapshot_cycle_sec_ = DEFAULT_SNAPSHOT_CYCLE_SEC;
  if (!snapshot_cycle_sec_str.empty())
    snapshot_cycle_sec_ = CONFIG_INTEGER(config::SNAPSHOT_CYCLE_SEC);

  std::string snapshot_folder_ = DEFAULT_SNAPSHOT_FOLDER;
  if (!snapshot_folder_str.empty()) snapshot_folder_ = snapshot_folder_str;

  if (snapshot_cycle_sec_ != -1) {
    auto create_snapshot = [this, snapshot_folder_,
                            max_retained_snapshots_]() -> void {
      GraphDbAccessor db_accessor(*this);
      snapshooter_.MakeSnapshot(db_accessor, fs::path(snapshot_folder_) / name_,
                                max_retained_snapshots_);
    };
    snapshot_creator_.Run(std::chrono::seconds(snapshot_cycle_sec_),
                          create_snapshot);
  }
}

GraphDb::~GraphDb() {
  // Stop the gc scheduler to not run into race conditions for deletions.
  gc_scheduler_.Stop();

  // Stop the snapshot creator to avoid snapshooting while database is beeing
  // deleted.
  snapshot_creator_.Stop();

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
