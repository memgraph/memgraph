#pragma once

#include <memory>
#include <mutex>

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"
#include "database/counters.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/key_index.hpp"
#include "database/indexes/label_property_index.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "durability/wal.hpp"
#include "io/network/network_endpoint.hpp"
#include "mvcc/version_list.hpp"
#include "storage/concurrent_id_mapper.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "storage/deferred_deleter.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"
#include "utils/scheduler.hpp"

/**
 * Main class which represents Database concept in code.
 * This class is essentially a data structure. It exposes
 * all the data publicly, and should therefore not be directly
 * exposed to client functions. The GraphDbAccessor is used for that.
 *
 * Always be sure that GraphDb object is destructed before main exits, i. e.
 * GraphDb object shouldn't be part of global/static variable, except if its
 * destructor is explicitly called before main exits. Consider code:
 *
 * GraphDb db;  // KeyIndex is created as a part of db.
 * int main() {
 *   GraphDbAccessor dba(db);
 *   auto v = dba.InsertVertex();
 *   v.add_label(dba.Label(
 *       "Start"));  // New SkipList is created in KeyIndex for LabelIndex.
 *                   // That SkipList creates SkipListGc which
 *                   // initialises static Executor object.
 *   return 0;
 * }
 *
 * After main exits: 1. Executor is destructed, 2. KeyIndex is destructed.
 * Destructor of KeyIndex calls delete on created SkipLists which destroy
 * SkipListGc that tries to use Excutioner object that doesn't exist anymore.
 * -> CRASH
 */
class GraphDb {
  using Endpoint = io::network::NetworkEndpoint;

 public:
  /// GraphDb configuration. Initialized from flags, but modifiable.
  struct Config {
    Config();
    // Durability flags.
    bool durability_enabled;
    std::string durability_directory;
    bool db_recover_on_startup;
    int snapshot_cycle_sec;
    int snapshot_max_retained;
    int snapshot_on_exit;

    // Misc flags.
    int gc_cycle_sec;
    int query_execution_time_sec;
  };

  /** Single-node GraphDb ctor. */
  explicit GraphDb(Config config = Config{});

  /** Distributed master GraphDb ctor. */
  GraphDb(communication::messaging::System &system,
          distributed::MasterCoordination &master, Config config = Config());

  /** Distributed worker GraphDb ctor. */
  GraphDb(communication::messaging::System &system, int worker_id,
          distributed::WorkerCoordination &worker, Endpoint master_endpoint,
          Config config = Config());

 private:
  // Private ctor used by other ctors. */
  GraphDb(Config config, int worker_id);

 public:
  /** Delete all vertices and edges and free all deferred deleters. */
  ~GraphDb();

  GraphDb(const GraphDb &db) = delete;
  GraphDb(GraphDb &&other) = delete;
  GraphDb &operator=(const GraphDb &other) = delete;
  GraphDb &operator=(GraphDb &&other) = delete;

  /** Stop all transactions and set is_accepting_transactions_ to false. */
  void Shutdown();

  void CollectGarbage();

  gid::Generator &VertexGenerator() { return vertex_generator_; }
  gid::Generator &EdgeGenerator() { return edge_generator_; }

  /** When this is false, no new transactions should be created. */
  std::atomic<bool> is_accepting_transactions_{true};

 private:
  friend class GraphDbAccessor;

  Config config_;

  /** Transaction engine related to this database. Master instance if this
   * GraphDb is a single-node deployment, or the master in a distributed system.
   * Otherwise a WorkerEngine instance. */
  std::unique_ptr<tx::Engine> tx_engine_;

  int worker_id_{0};

  gid::Generator vertex_generator_{worker_id_};
  gid::Generator edge_generator_{worker_id_};

  // main storage for the graph
  ConcurrentMap<gid::Gid, mvcc::VersionList<Vertex> *> vertices_;
  ConcurrentMap<gid::Gid, mvcc::VersionList<Edge> *> edges_;

  // Garbage collectors
  GarbageCollector<ConcurrentMap<gid::Gid, mvcc::VersionList<Vertex> *>, Vertex>
      gc_vertices_;
  GarbageCollector<ConcurrentMap<gid::Gid, mvcc::VersionList<Edge> *>, Edge>
      gc_edges_;

  // Deleters for not relevant records
  DeferredDeleter<Vertex> vertex_record_deleter_;
  DeferredDeleter<Edge> edge_record_deleter_;

  // Deleters for not relevant version_lists
  DeferredDeleter<mvcc::VersionList<Vertex>> vertex_version_list_deleter_;
  DeferredDeleter<mvcc::VersionList<Edge>> edge_version_list_deleter_;

  // Id to value mappers.
  // TODO this should be also garbage collected
  std::unique_ptr<storage::ConcurrentIdMapper<GraphDbTypes::Label>> labels_;
  std::unique_ptr<storage::ConcurrentIdMapper<GraphDbTypes::EdgeType>>
      edge_types_;
  std::unique_ptr<storage::ConcurrentIdMapper<GraphDbTypes::Property>>
      properties_;

  // indexes
  KeyIndex<GraphDbTypes::Label, Vertex> labels_index_;
  LabelPropertyIndex label_property_index_;

  // Set of transactions ids which are building indexes currently
  ConcurrentSet<tx::transaction_id_t> index_build_tx_in_progress_;

  durability::WriteAheadLog wal_;

  // Schedulers
  Scheduler gc_scheduler_;
  Scheduler snapshot_creator_;
  // Periodically wakes up and hints to transactions that are running for a long
  // time to stop their execution.
  Scheduler transaction_killer_;

  // DB level global counters, used in the "counter" function.
  std::unique_ptr<database::Counters> counters_;

  // Returns Endpoint info for worker ID. Different implementation in master vs.
  // worker. Unused in single-node version.
  std::function<io::network::NetworkEndpoint(int)> get_endpoint_;

  // Starts DB operations once all members have been constructed.
  void Start();
  // Starts periodically generating database snapshots.
  void StartSnapshooting();
};
