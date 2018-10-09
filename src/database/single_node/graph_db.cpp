#include "database/single_node/graph_db.hpp"

#include <experimental/optional>

#include <glog/logging.h>

#include "database/single_node/counters.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "durability/paths.hpp"
#include "durability/single_node/recovery.hpp"
#include "durability/single_node/snapshooter.hpp"
#include "storage/single_node/concurrent_id_mapper.hpp"
#include "storage/single_node/storage_gc.hpp"
#include "transactions/single_node/engine.hpp"
#include "utils/file.hpp"

namespace database {

GraphDb::GraphDb(Config config) : config_(config) {
  CHECK(config.worker_id == 0)
      << "Worker ID should only be set in distributed GraphDb";
  if (config_.durability_enabled) utils::CheckDir(config_.durability_directory);

  // Durability recovery.
  if (config_.db_recover_on_startup) {
    CHECK(durability::VersionConsistency(config_.durability_directory))
        << "Contents of durability directory are not compatible with the "
           "current version of Memgraph binary!";

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;
    durability::RecoveryData recovery_data;

    recovery_info = durability::RecoverOnlySnapshot(
        config_.durability_directory, this, &recovery_data,
        std::experimental::nullopt, 0);

    // Post-recovery setup and checking.
    if (recovery_info) {
      recovery_data.wal_tx_to_recover = recovery_info->wal_recovered;
      durability::RecoveryTransactions recovery_transactions(this);
      durability::RecoverWal(config_.durability_directory, this, &recovery_data,
                             &recovery_transactions);
      durability::RecoverIndexes(this, recovery_data.indexes);
    }
  }

  if (config_.durability_enabled) {
    // move any existing snapshots or wal files to a deprecated folder.
    if (!config_.db_recover_on_startup &&
        durability::ContainsDurabilityFiles(config_.durability_directory)) {
      durability::MoveToBackup(config_.durability_directory);
      LOG(WARNING) << "Since Memgraph was not supposed to recover on startup "
                      "and durability is enabled, your current durability "
                      "files will likely be overriden. To prevent important "
                      "data loss, Memgraph has stored those files into a "
                      ".backup directory inside durability directory";
    }
    wal_.Init();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(config_.snapshot_cycle_sec), [this] {
          auto dba = this->Access();
          this->MakeSnapshot(*dba);
        });
  }

  // Start transaction killer.
  if (config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        "TX killer",
        std::chrono::seconds(
            std::max(1, std::min(5, config_.query_execution_time_sec / 4))),
        [this]() {
          tx_engine_.LocalForEachActiveTransaction([this](tx::Transaction &t) {
            if (t.creation_time() +
                    std::chrono::seconds(config_.query_execution_time_sec) <
                std::chrono::steady_clock::now()) {
              t.set_should_abort();
            };
          });
        });
  }
}

GraphDb::~GraphDb() {
  snapshot_creator_ = nullptr;

  is_accepting_transactions_ = false;
  tx_engine_.LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });

  if (config_.snapshot_on_exit) {
    auto dba = this->Access();
    MakeSnapshot(*dba);
  }
}

std::unique_ptr<GraphDbAccessor> GraphDb::Access() {
  // NOTE: We are doing a heap allocation to allow polymorphism. If this poses
  // performance issues, we may want to have a stack allocated GraphDbAccessor
  // which is constructed with a pointer to some global implementation struct
  // which contains only pure functions (without any state).
  return std::unique_ptr<GraphDbAccessor>(new GraphDbAccessor(*this));
}

std::unique_ptr<GraphDbAccessor> GraphDb::Access(tx::TransactionId tx_id) {
  return std::unique_ptr<GraphDbAccessor>(new GraphDbAccessor(*this, tx_id));
}

Storage &GraphDb::storage() { return *storage_; }

durability::WriteAheadLog &GraphDb::wal() { return wal_; }

tx::Engine &GraphDb::tx_engine() { return tx_engine_; }

storage::ConcurrentIdMapper<storage::Label> &GraphDb::label_mapper() {
  return label_mapper_;
}

storage::ConcurrentIdMapper<storage::EdgeType> &GraphDb::edge_type_mapper() {
  return edge_mapper_;
}

storage::ConcurrentIdMapper<storage::Property> &GraphDb::property_mapper() {
  return property_mapper_;
}

database::Counters &GraphDb::counters() { return counters_; }

void GraphDb::CollectGarbage() { storage_gc_->CollectGarbage(); }

bool GraphDb::MakeSnapshot(GraphDbAccessor &accessor) {
  const bool status = durability::MakeSnapshot(
      *this, accessor, 0, fs::path(config_.durability_directory),
      config_.snapshot_max_retained);
  if (status) {
    LOG(INFO) << "Snapshot created successfully.";
  } else {
    LOG(ERROR) << "Snapshot creation failed!";
  }
  return status;
}

void GraphDb::ReinitializeStorage() {
  // Release gc scheduler to stop it from touching storage
  storage_gc_ = nullptr;
  storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  storage_gc_ =
      std::make_unique<StorageGc>(*storage_, tx_engine_, config_.gc_cycle_sec);
}

}  // namespace database
