#pragma once

#include <thread>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/key_index.hpp"
#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/unique_object_store.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"
#include "utils/pass_key.hpp"

// TODO: Maybe split this in another layer between Db and Dbms. Where the new
// layer would hold SnapshotEngine and his kind of concept objects. Some
// guidelines would be: retain objects which are necessary to implement querys
// in Db, the rest can be moved to the new layer.
/**
 * Main class which represents Database concept in code.
 * This class is essentially a data structure. It exposes
 * all the data publicly, and should therefore not be directly
 * exposed to client functions. The GraphDbAccessor is used for that.
 */
class GraphDb {
 public:
  /**
   * Construct database with a custom name.
   *
   * @param name database name
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  GraphDb(const std::string &name, bool import_snapshot = true);

  /**
   * Database object can't be copied.
   */
  GraphDb(const GraphDb &db) = delete;

  /** transaction engine related to this database */
  tx::Engine tx_engine;

  /** garbage collector related to this database*/
  // TODO bring back garbage collection
  //  Garbage garbage = {tx_engine};

  // TODO bring back shapshot engine
  //  SnapshotEngine snap_engine = {*this};

  // database name
  // TODO consider if this is even necessary
  const std::string name_;

  // main storage for the graph
  SkipList<mvcc::VersionList<Vertex> *> vertices_;
  SkipList<mvcc::VersionList<Edge> *> edges_;
  GarbageCollector<Vertex> gc_vertices_;
  GarbageCollector<Edge> gc_edges_;

  // unique object stores
  // TODO this should be also garbage collected
  ConcurrentSet<std::string> labels_;
  ConcurrentSet<std::string> edge_types_;
  ConcurrentSet<std::string> properties_;

  // indexes
  KeyIndex<GraphDbTypes::Label, Vertex> labels_index_;
  KeyIndex<GraphDbTypes::EdgeType, Edge> edge_types_index_;
};
