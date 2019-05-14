#include "database/single_node_ha/graph_db.hpp"

#include <optional>

#include <glog/logging.h>

#include "database/single_node_ha/graph_db_accessor.hpp"
#include "storage/single_node_ha/concurrent_id_mapper.hpp"
#include "storage/single_node_ha/storage_gc.hpp"
#include "transactions/single_node_ha/engine.hpp"

namespace database {

GraphDb::GraphDb(Config config) : config_(config) {}

void GraphDb::Start() {
  utils::EnsureDirOrDie(config_.durability_directory);
  raft_server_.Start();
  storage_info_.Start();
  CHECK(coordination_.Start()) << "Couldn't start coordination!";

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

void GraphDb::AwaitShutdown(std::function<void(void)> call_before_shutdown) {
  coordination_.AwaitShutdown([this, &call_before_shutdown]() {
    tx_engine_.LocalForEachActiveTransaction(
        [](auto &t) { t.set_should_abort(); });

    call_before_shutdown();

    raft_server_.Shutdown();
  });
}

void GraphDb::Shutdown() { coordination_.Shutdown(); }

GraphDbAccessor GraphDb::Access() {
  return GraphDbAccessor(this);
}

GraphDbAccessor GraphDb::Access(tx::TransactionId tx_id) {
  return GraphDbAccessor(this, tx_id);
}

GraphDbAccessor GraphDb::AccessBlocking(
    std::optional<tx::TransactionId> parent_tx) {
  return GraphDbAccessor(this, parent_tx);
}

Storage &GraphDb::storage() { return *storage_; }

raft::RaftInterface *GraphDb::raft() { return &raft_server_; }

raft::StorageInfo *GraphDb::storage_info() { return &storage_info_; }

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

void GraphDb::CollectGarbage() { storage_gc_->CollectGarbage(); }

void GraphDb::Reset() {
  // Release gc scheduler to stop it from touching storage.
  storage_gc_ = nullptr;

  // This will make all active transactions to abort and reset the internal
  // state.
  tx_engine_.Reset();

  storage_ = std::make_unique<Storage>(config_.properties_on_disk);
  storage_gc_ = std::make_unique<StorageGc>(
      *storage_, tx_engine_, &raft_server_, config_.gc_cycle_sec);
}

}  // namespace database
