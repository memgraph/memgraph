/// @file
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "database/counters.hpp"
#include "durability/distributed/recovery.hpp"
#include "durability/distributed/wal.hpp"
#include "io/network/endpoint.hpp"
#include "storage/common/concurrent_id_mapper.hpp"
#include "storage/common/types.hpp"
#include "storage/distributed/storage.hpp"
#include "storage/distributed/storage_gc.hpp"
#include "storage/distributed/vertex_accessor.hpp"
#include "transactions/engine.hpp"
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
  virtual database::Counters &counters() = 0;
  virtual void CollectGarbage() = 0;

  /// Makes a snapshot from the visibility of the given accessor
  virtual bool MakeSnapshot(GraphDbAccessor &accessor) = 0;

  /// Releases the storage object safely and creates a new object.
  /// This is needed because of recovery, otherwise we might try to recover into
  /// a storage which has already been polluted because of a failed previous
  /// recovery
  virtual void ReinitializeStorage() = 0;

  /// When this is false, no new transactions should be created.
  bool is_accepting_transactions() const { return is_accepting_transactions_; }

 protected:
  std::atomic<bool> is_accepting_transactions_{true};
};

namespace impl {
class SingleNode;
}  // namespace impl

class SingleNode final : public GraphDb {
 public:
  explicit SingleNode(Config config = Config());
  ~SingleNode();

  std::unique_ptr<GraphDbAccessor> Access() override;
  std::unique_ptr<GraphDbAccessor> Access(tx::TransactionId) override;

  Storage &storage() override;
  durability::WriteAheadLog &wal() override;
  tx::Engine &tx_engine() override;
  storage::ConcurrentIdMapper<storage::Label> &label_mapper() override;
  storage::ConcurrentIdMapper<storage::EdgeType> &edge_type_mapper() override;
  storage::ConcurrentIdMapper<storage::Property> &property_mapper() override;
  database::Counters &counters() override;
  void CollectGarbage() override;

  bool MakeSnapshot(GraphDbAccessor &accessor) override;
  void ReinitializeStorage() override;

 private:
  std::unique_ptr<impl::SingleNode> impl_;

  std::unique_ptr<utils::Scheduler> snapshot_creator_;
  utils::Scheduler transaction_killer_;
};

class SingleNodeRecoveryTransanctions final
    : public durability::RecoveryTransactions {
 public:
  explicit SingleNodeRecoveryTransanctions(SingleNode *db);
  ~SingleNodeRecoveryTransanctions();

  void Begin(const tx::TransactionId &tx_id) override;
  void Abort(const tx::TransactionId &tx_id) override;
  void Commit(const tx::TransactionId &tx_id) override;
  void Apply(const database::StateDelta &delta) override;

 private:
  SingleNode *db_;
  std::unordered_map<tx::TransactionId, std::unique_ptr<GraphDbAccessor>>
      accessors_;
};

}  // namespace database
