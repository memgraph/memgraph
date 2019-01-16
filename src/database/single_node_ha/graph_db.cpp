#include "database/single_node_ha/graph_db.hpp"

#include <experimental/optional>

#include <glog/logging.h>

#include "database/single_node_ha/counters.hpp"
#include "database/single_node_ha/graph_db_accessor.hpp"
#include "storage/single_node_ha/concurrent_id_mapper.hpp"
#include "storage/single_node_ha/storage_gc.hpp"
#include "transactions/single_node_ha/engine.hpp"

namespace database {

GraphDb::GraphDb(Config config) : config_(config) {}

void GraphDb::Start() {
  utils::CheckDir(config_.durability_directory);
  raft_server_.Start();
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

GraphDb::~GraphDb() {}

bool GraphDb::AwaitShutdown(std::function<void(void)> call_before_shutdown) {
  bool ret =
      coordination_.AwaitShutdown([this, &call_before_shutdown]() -> bool {
        is_accepting_transactions_ = false;
        tx_engine_.LocalForEachActiveTransaction(
            [](auto &t) { t.set_should_abort(); });

        call_before_shutdown();

        return true;
      });

  return ret;
}

void GraphDb::Shutdown() {
  raft_server_.Shutdown();
  coordination_.Shutdown();
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

std::unique_ptr<GraphDbAccessor> GraphDb::AccessBlocking(
    std::experimental::optional<tx::TransactionId> parent_tx) {
  return std::unique_ptr<GraphDbAccessor>(
      new GraphDbAccessor(*this, parent_tx));
}

Storage &GraphDb::storage() { return *storage_; }

raft::RaftInterface *GraphDb::raft() { return &raft_server_; }

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

void GraphDb::Reset() {
  // Release gc scheduler to stop it from touching storage.
  storage_gc_ = nullptr;
  storage_ = std::make_unique<Storage>(config_.properties_on_disk);

  // This will make all active transactions to abort and reset the internal
  // state.
  tx_engine_.Reset();

  storage_gc_ = std::make_unique<StorageGc>(
      *storage_, tx_engine_, &raft_server_, config_.gc_cycle_sec);
}

}  // namespace database
