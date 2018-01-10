#include <experimental/filesystem>
#include <functional>

#include <glog/logging.h>

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/timer.hpp"

namespace fs = std::experimental::filesystem;

#define INIT_MAPPERS(type, ...)                                              \
  labels_ = std::make_unique<type<GraphDbTypes::Label>>(__VA_ARGS__);        \
  edge_types_ = std::make_unique<type<GraphDbTypes::EdgeType>>(__VA_ARGS__); \
  properties_ = std::make_unique<type<GraphDbTypes::Property>>(__VA_ARGS__);

GraphDb::GraphDb(Config config) : GraphDb(config, 0) {
  tx_engine_ = std::make_unique<tx::MasterEngine>(&wal_);
  counters_ = std::make_unique<database::SingleNodeCounters>();
  INIT_MAPPERS(storage::SingleNodeConcurrentIdMapper);
  Start();
}

GraphDb::GraphDb(communication::messaging::System &system,
                 distributed::MasterCoordination &master, Config config)
    : GraphDb(config, 0) {
  auto tx_engine = std::make_unique<tx::MasterEngine>(&wal_);
  tx_engine->StartServer(system);
  tx_engine_ = std::move(tx_engine);
  auto counters = std::make_unique<database::MasterCounters>(system);
  counters_ = std::move(counters);
  INIT_MAPPERS(storage::MasterConcurrentIdMapper, system);
  get_endpoint_ = [&master](int worker_id) {
    return master.GetEndpoint(worker_id);
  };
  Start();
}

GraphDb::GraphDb(communication::messaging::System &system, int worker_id,
                 distributed::WorkerCoordination &worker,
                 Endpoint master_endpoint, Config config)
    : GraphDb(config, worker_id) {
  tx_engine_ = std::make_unique<tx::WorkerEngine>(system, master_endpoint);
  counters_ =
      std::make_unique<database::WorkerCounters>(system, master_endpoint);
  INIT_MAPPERS(storage::WorkerConcurrentIdMapper, system, master_endpoint);
  get_endpoint_ = [&worker](int worker_id) {
    return worker.GetEndpoint(worker_id);
  };
  Start();
}

#undef INIT_MAPPERS

GraphDb::GraphDb(Config config, int worker_id)
    : config_(config),
      worker_id_(worker_id),
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_),
      wal_{config.durability_directory, config.durability_enabled} {}

void GraphDb::Start() {
  // Pause of -1 means we shouldn't run the GC.
  if (config_.gc_cycle_sec != -1) {
    gc_scheduler_.Run(std::chrono::seconds(config_.gc_cycle_sec),
                      [this]() { CollectGarbage(); });
  }

  // If snapshots are enabled we need the durability dir.
  if (config_.durability_enabled)
    durability::CheckDurabilityDir(config_.durability_directory);

  if (config_.db_recover_on_startup)
    durability::Recover(config_.durability_directory, *this);
  if (config_.durability_enabled) wal_.Enable();
  StartSnapshooting();

  if (config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        std::chrono::seconds(
            std::max(1, std::min(5, config_.query_execution_time_sec / 4))),
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
      if (!durability::MakeSnapshot(*this,
                                    fs::path(config_.durability_directory),
                                    config_.snapshot_max_retained)) {
        LOG(WARNING) << "Durability: snapshot creation failed";
      }
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

  // Stop the snapshot creator to avoid snapshooting while database is being
  // deleted.
  snapshot_creator_.Stop();

  // Stop transaction killer.
  transaction_killer_.Stop();

  // Create last database snapshot
  if (config_.snapshot_on_exit == true) {
    LOG(INFO) << "Creating snapshot on shutdown..." << std::endl;
    const bool status =
        durability::MakeSnapshot(*this, fs::path(config_.durability_directory),
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
