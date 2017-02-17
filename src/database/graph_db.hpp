#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "transactions/engine.hpp"
#include "mvcc/version_list.hpp"
#include "utils/pass_key.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"
#include "storage/unique_object_store.hpp"

// forward declaring Edge and Vertex because they use
// GraphDb::Label etc., and therefore include this header
class Vertex;
class VertexAccessor;
class Edge;
class EdgeAccessor;

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

  // definitions for what data types are used for a Label, Property, EdgeType
  using Label = std::string*;
  using EdgeType = std::string*;
  using Property = std::string*;

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
  SkipList<mvcc::VersionList<Vertex>*> vertices_;
  SkipList<mvcc::VersionList<Edge>*> edges_;

  // unique object stores
  ConcurrentSet<std::string> labels_;
  ConcurrentSet<std::string> edge_types_;
  ConcurrentSet<std::string> properties_;
};
