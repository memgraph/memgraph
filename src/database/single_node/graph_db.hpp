/// @file
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "database/single_node/counters.hpp"
#include "durability/single_node/recovery.hpp"
#include "durability/single_node/wal.hpp"
#include "io/network/endpoint.hpp"
#include "storage/common/types.hpp"
#include "storage/single_node/concurrent_id_mapper.hpp"
#include "storage/single_node/storage.hpp"
#include "storage/single_node/storage_gc.hpp"
#include "transactions/single_node/engine.hpp"
#include "utils/scheduler.hpp"

namespace database {

/// Database configuration. Initialized from flags, but modifiable.
struct Config {
  Config();

  // Durability flags.
  bool durability_enabled;
  std::string durability_directory;
  bool db_recover_on_startup;
  int snapshot_cycle_sec;
  int snapshot_max_retained;
  int snapshot_on_exit;
  bool synchronous_commit;

  // Misc flags.
  int gc_cycle_sec;
  int query_execution_time_sec;

  // set of properties which will be stored on disk
  std::vector<std::string> properties_on_disk;

  // Distributed master/worker flags.
  bool dynamic_graph_partitioner_enabled{false};
  int rpc_num_client_workers{0};
  int rpc_num_server_workers{0};
  int worker_id{0};
  io::network::Endpoint master_endpoint{"0.0.0.0", 0};
  io::network::Endpoint worker_endpoint{"0.0.0.0", 0};
  int recovering_cluster_size{0};
};

class GraphDbAccessor;

/// An abstract base class providing the interface for a graph database.
///
/// Always be sure that GraphDb object is destructed before main exits, i. e.
/// GraphDb object shouldn't be part of global/static variable, except if its
/// destructor is explicitly called before main exits. Consider code:
///
/// GraphDb db;  // KeyIndex is created as a part of database::Storage
/// int main() {
///   GraphDbAccessor dba(db);
///   auto v = dba.InsertVertex();
///   v.add_label(dba.Label(
///       "Start"));  // New SkipList is created in KeyIndex for LabelIndex.
///                   // That SkipList creates SkipListGc which
///                   // initialises static Executor object.
///   return 0;
/// }
///
/// After main exits: 1. Executor is destructed, 2. KeyIndex is destructed.
/// Destructor of KeyIndex calls delete on created SkipLists which destroy
/// SkipListGc that tries to use Excutioner object that doesn't exist anymore.
/// -> CRASH
class GraphDb {
 public:
  explicit GraphDb(Config config = Config());
  ~GraphDb();

  GraphDb(const GraphDb &) = delete;
  GraphDb(GraphDb &&) = delete;
  GraphDb &operator=(const GraphDb &) = delete;
  GraphDb &operator=(GraphDb &&) = delete;

  /// Create a new accessor by starting a new transaction.
  std::unique_ptr<GraphDbAccessor> Access();
  /// Create an accessor for a running transaction.
  std::unique_ptr<GraphDbAccessor> Access(tx::TransactionId);

  Storage &storage();
  durability::WriteAheadLog &wal();
  tx::Engine &tx_engine();
  storage::ConcurrentIdMapper<storage::Label> &label_mapper();
  storage::ConcurrentIdMapper<storage::EdgeType> &edge_type_mapper();
  storage::ConcurrentIdMapper<storage::Property> &property_mapper();
  database::Counters &counters();
  void CollectGarbage();

  /// Makes a snapshot from the visibility of the given accessor
  bool MakeSnapshot(GraphDbAccessor &accessor);

  /// Releases the storage object safely and creates a new object.
  /// This is needed because of recovery, otherwise we might try to recover into
  /// a storage which has already been polluted because of a failed previous
  /// recovery
  void ReinitializeStorage();

  /// When this is false, no new transactions should be created.
  bool is_accepting_transactions() const { return is_accepting_transactions_; }

 protected:
  std::atomic<bool> is_accepting_transactions_{true};

  std::unique_ptr<utils::Scheduler> snapshot_creator_;
  utils::Scheduler transaction_killer_;

  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{
      config_.worker_id, config_.durability_directory,
      config_.durability_enabled, config_.synchronous_commit};

  tx::Engine tx_engine_{&wal_};
  std::unique_ptr<StorageGc> storage_gc_ =
      std::make_unique<StorageGc>(*storage_, tx_engine_, config_.gc_cycle_sec);
  storage::ConcurrentIdMapper<storage::Label> label_mapper_{
      storage_->PropertiesOnDisk()};
  storage::ConcurrentIdMapper<storage::EdgeType> edge_mapper_{
      storage_->PropertiesOnDisk()};
  storage::ConcurrentIdMapper<storage::Property> property_mapper_{
      storage_->PropertiesOnDisk()};
  database::Counters counters_;
};

}  // namespace database
