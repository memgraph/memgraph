#pragma once

#include <thread>

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
#include "storage/deferred_deleter.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/unique_object_store.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"
#include "utils/scheduler.hpp"

namespace fs = std::experimental::filesystem;

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
  GraphDb();
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

  /** transaction engine related to this database */
  tx::Engine tx_engine_;

  std::atomic<int64_t> next_vertex_id_{0};
  std::atomic<int64_t> next_edge_id{0};

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

  // unique object stores
  // TODO this should be also garbage collected
  ConcurrentSet<std::string> labels_;
  ConcurrentSet<std::string> edge_types_;
  ConcurrentSet<std::string> properties_;

  // indexes
  KeyIndex<GraphDbTypes::Label, Vertex> labels_index_;
  LabelPropertyIndex label_property_index_;

  /**
   * Flag indicating if index building is in progress. Memgraph does not support
   * concurrent index builds on the same database (transaction engine), so we
   * reject index builds if there is one in progress. See
   * GraphDbAccessor::BuildIndex.
   */
  std::atomic<bool> index_build_in_progress_{false};

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
