#pragma once

#include <memory>

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/key_index.hpp"
#include "database/indexes/label_property_index.hpp"
#include "durability/wal.hpp"
#include "mvcc/version_list.hpp"
#include "storage/concurrent_id_mapper.hpp"
#include "storage/deferred_deleter.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
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

  explicit GraphDb(Config config = Config{});
  /** Delete all vertices and edges and free all deferred deleters. */
  ~GraphDb();

  /** Database object can't be copied. */
  GraphDb(const GraphDb &db) = delete;
  GraphDb(GraphDb &&other) = default;
  GraphDb &operator=(const GraphDb &other) = default;
  GraphDb &operator=(GraphDb &&other) = default;

  /** Stop all transactions and set is_accepting_transactions_ to false. */
  void Shutdown();

  void CollectGarbage();

  /** When this is false, no new transactions should be created. */
  std::atomic<bool> is_accepting_transactions_{true};

 private:
  friend class GraphDbAccessor;

  void StartSnapshooting();

  Config config_;

  /** Transaction engine related to this database. Master instance if this
   * GraphDb is a single-node deployment, or the master in a distributed system.
   * Otherwise a WorkerEngine instance. */
  std::unique_ptr<tx::Engine> tx_engine_;

  std::atomic<int64_t> next_vertex_id_{0};
  std::atomic<int64_t> next_edge_id_{0};

  // main storage for the graph
  ConcurrentMap<int64_t, mvcc::VersionList<Vertex> *> vertices_;
  ConcurrentMap<int64_t, mvcc::VersionList<Edge> *> edges_;

  // Garbage collectors
  GarbageCollector<ConcurrentMap<int64_t, mvcc::VersionList<Vertex> *>, Vertex>
      gc_vertices_;
  GarbageCollector<ConcurrentMap<int64_t, mvcc::VersionList<Edge> *>, Edge>
      gc_edges_;

  // Deleters for not relevant records
  DeferredDeleter<Vertex> vertex_record_deleter_;
  DeferredDeleter<Edge> edge_record_deleter_;

  // Deleters for not relevant version_lists
  DeferredDeleter<mvcc::VersionList<Vertex>> vertex_version_list_deleter_;
  DeferredDeleter<mvcc::VersionList<Edge>> edge_version_list_deleter_;

  // Id to value mappers.
  // TODO this should be also garbage collected
  std::unique_ptr<ConcurrentIdMapper<GraphDbTypes::Label, std::string>> labels_{
      new MasterConcurrentIdMapper<GraphDbTypes::Label, std::string>};
  std::unique_ptr<ConcurrentIdMapper<GraphDbTypes::EdgeType, std::string>>
      edge_types_{
          new MasterConcurrentIdMapper<GraphDbTypes::EdgeType, std::string>};
  std::unique_ptr<ConcurrentIdMapper<GraphDbTypes::Property, std::string>>
      properties_{
          new MasterConcurrentIdMapper<GraphDbTypes::Property, std::string>};

  // indexes
  KeyIndex<GraphDbTypes::Label, Vertex> labels_index_;
  LabelPropertyIndex label_property_index_;

  /**
   * Set of transactions ids which are building indexes currently
   */
  ConcurrentSet<tx::transaction_id_t> index_build_tx_in_progress_;

  durability::WriteAheadLog wal_;

  // Schedulers
  Scheduler gc_scheduler_;
  Scheduler snapshot_creator_;
  // Periodically wakes up and hints to transactions that are running for a long
  // time to stop their execution.
  Scheduler transaction_killer_;

  /** DB level global counters, used in the "counter" function. */
  ConcurrentMap<std::string, std::atomic<int64_t>> counters_;
};
