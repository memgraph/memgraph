#include <functional>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/recovery.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "utils/timer.hpp"

DEFINE_int32(gc_cycle_sec, 30,
             "Amount of time between starts of two cleaning cycles in seconds. "
             "-1 to turn off.");
DEFINE_int32(snapshot_max_retained, -1,
             "Number of retained snapshots, -1 means without limit.");
DEFINE_int32(snapshot_cycle_sec, -1,
             "Amount of time between starts of two snapshooters in seconds. -1 "
             "to turn off.");
DEFINE_int32(query_execution_time_sec, 180,
             "Maximum allowed query execution time. Queries exceeding this "
             "limit will be aborted. Value of -1 means no limit.");

DEFINE_bool(snapshot_on_exit, false, "Snapshot on exiting the database.");

DECLARE_string(snapshot_directory);

GraphDb::GraphDb(const std::string &name, const fs::path &snapshot_db_dir)
    : name_(name),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_) {
  // Pause of -1 means we shouldn't run the GC.
  if (FLAGS_gc_cycle_sec != -1) {
    gc_scheduler_.Run(std::chrono::seconds(FLAGS_gc_cycle_sec),
                      [this]() { CollectGarbage(); });
  }

  RecoverDatabase(snapshot_db_dir);
  StartSnapshooting();

  if (FLAGS_query_execution_time_sec != -1) {
    transaction_killer_.Run(
        std::chrono::seconds(
            std::max(1, std::min(5, FLAGS_query_execution_time_sec / 4))),
        [this]() {
          tx_engine_.ForEachActiveTransaction([](tx::Transaction &t) {
            if (t.creation_time() +
                    std::chrono::seconds(FLAGS_query_execution_time_sec) <
                std::chrono::steady_clock::now()) {
              t.set_should_abort();
            };
          });
        });
  }
}

void GraphDb::StartSnapshooting() {
  if (FLAGS_snapshot_cycle_sec != -1) {
    auto create_snapshot = [this]() -> void {
      GraphDbAccessor db_accessor(*this);
      snapshooter_.MakeSnapshot(db_accessor,
                                fs::path(FLAGS_snapshot_directory) / name_,
                                FLAGS_snapshot_max_retained);
    };
    snapshot_creator_.Run(std::chrono::seconds(FLAGS_snapshot_cycle_sec),
                          create_snapshot);
  }
}

void GraphDb::RecoverDatabase(const fs::path &snapshot_db_dir) {
  if (snapshot_db_dir.empty()) return;
  std::vector<fs::path> snapshots;
  for (auto &snapshot_file : fs::directory_iterator(snapshot_db_dir)) {
    if (fs::is_regular_file(snapshot_file)) {
      snapshots.push_back(snapshot_file);
    }
  }

  std::sort(snapshots.rbegin(), snapshots.rend());
  Recovery recovery;
  for (auto &snapshot_file : snapshots) {
    GraphDbAccessor db_accessor(*this);
    std::cout << "Starting database recovery from snapshot " << snapshot_file
              << std::endl;
    if (recovery.Recover(snapshot_file.string(), db_accessor)) {
      std::cout << "Recovery successful." << std::endl;
      return;
    } else {
      LOG(ERROR) << "Recovery unsuccessful, trying older snapshot..."
                 << std::endl;
    }
  }
}

void GraphDb::CollectGarbage() {
  // main garbage collection logic
  // see wiki documentation for logic explanation
  LOG(INFO) << "Garbage collector started";
  const auto snapshot = tx_engine_.GcSnapshot();
  {
    // This can be run concurrently
    utils::Timer x;
    gc_vertices_.Run(snapshot, tx_engine_);
    gc_edges_.Run(snapshot, tx_engine_);
    VLOG(1) << "Garbage collector mvcc phase time: " << x.Elapsed().count();
  }
  // This has to be run sequentially after gc because gc modifies
  // version_lists and changes the oldest visible record, on which Refresh
  // depends.
  {
    // This can be run concurrently
    utils::Timer x;
    labels_index_.Refresh(snapshot, tx_engine_);
    edge_types_index_.Refresh(snapshot, tx_engine_);
    label_property_index_.Refresh(snapshot, tx_engine_);
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
    const auto snapshot = tx_engine_.GcSnapshot();
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
  if (FLAGS_snapshot_on_exit == true) {
    GraphDbAccessor db_accessor(*this);
    LOG(INFO) << "Creating snapshot on shutdown..." << std::endl;
    const bool status = snapshooter_.MakeSnapshot(
        db_accessor, fs::path(FLAGS_snapshot_directory) / name_,
        FLAGS_snapshot_max_retained);
    if (status) {
      std::cout << "Snapshot created successfully." << std::endl;
    } else {
      LOG(ERROR) << "Snapshot creation failed!" << std::endl;
    }
  }

  // Delete vertices and edges which weren't collected before, also deletes
  // records inside version list
  for (auto &vertex : vertices_.access()) delete vertex;
  for (auto &edge : edges_.access()) delete edge;

  // Free expired records with the maximal possible id from all the deleters.
  edge_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  vertex_record_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  edge_version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
  vertex_version_list_deleter_.FreeExpiredObjects(tx::Transaction::MaxId());
}
