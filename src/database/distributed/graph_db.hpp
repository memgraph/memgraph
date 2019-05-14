/// @file
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "durability/distributed/recovery.hpp"
#include "durability/distributed/wal.hpp"
#include "io/network/endpoint.hpp"
#include "storage/common/types/types.hpp"
#include "storage/distributed/concurrent_id_mapper.hpp"
#include "storage/distributed/storage.hpp"
#include "storage/distributed/storage_gc.hpp"
#include "storage/distributed/vertex_accessor.hpp"
#include "transactions/distributed/engine.hpp"
#include "utils/scheduler.hpp"

namespace distributed {
class BfsRpcServer;
class BfsRpcClients;
class DataRpcServer;
class DataRpcClients;
class PlanDispatcher;
class PlanConsumer;
class PullRpcClients;
class ProduceRpcServer;
class UpdatesRpcServer;
class UpdatesRpcClients;
class DataManager;
class IndexRpcClients;
}  // namespace distributed

namespace database {
namespace impl {
class Master;
class Worker;
}  // namespace impl

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

  // Sizes of caches that hold remote data
  // Default value is same as in config.cpp
  size_t vertex_cache_size{5000};
  size_t edge_cache_size{5000};
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
  GraphDb() {}
  GraphDb(const GraphDb &) = delete;
  GraphDb(GraphDb &&) = delete;
  GraphDb &operator=(const GraphDb &) = delete;
  GraphDb &operator=(GraphDb &&) = delete;

  virtual ~GraphDb() {}

  /// Create a new accessor by starting a new transaction.
  virtual std::unique_ptr<GraphDbAccessor> Access() = 0;
  /// Create an accessor for a running transaction.
  virtual std::unique_ptr<GraphDbAccessor> Access(tx::TransactionId) = 0;

  virtual Storage &storage() = 0;
  virtual durability::WriteAheadLog &wal() = 0;
  virtual tx::Engine &tx_engine() = 0;
  virtual storage::ConcurrentIdMapper<storage::Label> &label_mapper() = 0;
  virtual storage::ConcurrentIdMapper<storage::EdgeType>
      &edge_type_mapper() = 0;
  virtual storage::ConcurrentIdMapper<storage::Property> &property_mapper() = 0;
  virtual void CollectGarbage() = 0;

  /// Makes a snapshot from the visibility of the given accessor
  virtual bool MakeSnapshot(GraphDbAccessor &accessor) = 0;

  /// Releases the storage object safely and creates a new object.
  /// This is needed because of recovery, otherwise we might try to recover into
  /// a storage which has already been polluted because of a failed previous
  /// recovery
  virtual void ReinitializeStorage() = 0;

  virtual int WorkerId() const = 0;
  virtual std::vector<int> GetWorkerIds() const = 0;

  virtual distributed::BfsRpcClients &bfs_subcursor_clients() = 0;
  virtual distributed::DataRpcClients &data_clients() = 0;
  virtual distributed::UpdatesRpcServer &updates_server() = 0;
  virtual distributed::UpdatesRpcClients &updates_clients() = 0;
  virtual distributed::DataManager &data_manager() = 0;

  /// When this is false, no new transactions should be created.
  bool is_accepting_transactions() const { return is_accepting_transactions_; }

 protected:
  std::atomic<bool> is_accepting_transactions_{true};
};
}  // namespace database
