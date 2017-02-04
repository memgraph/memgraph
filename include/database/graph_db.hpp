#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "database/db_transaction.hpp"
#include "snapshot/snapshot_engine.hpp"
#include "storage/garbage/garbage.hpp"
#include "transactions/engine.hpp"
#include "mvcc/version_list.hpp"
#include "utils/pass_key.hpp"

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
 */
class GraphDb {
public:
  using sptr = std::shared_ptr<GraphDb>;

  // definitions for what data types are used for a Label, Property, EdgeType
  using Label = uint32_t;
  using EdgeType = uint32_t;
  using Property = uint32_t;

  /**
   * This constructor will create a database with the name "default"
   *
   * NOTE: explicit is here to prevent compiler from evaluating const char *
   * into a bool.
   *
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  explicit GraphDb(bool import_snapshot = true);

  /**
   * Construct database with a custom name.
   *
   * @param name database name
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  GraphDb(const char *name, bool import_snapshot = true);

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

  /**
   * Creates a new Vertex and returns an accessor to it.
   *
   * @param db_trans The transaction that is creating a vertex.
   * @return See above.
   */
  VertexAccesor insert_vertex(DbTransaction& db_trans);

  /**
   * Creates a new Edge and returns an accessor to it.
   *
   * @param db_trans The transaction that is creating an Edge.
   * @param from The 'from' vertex.
   * @param to The 'to' vertex'
   * @param type Edge type.
   * @return  An accessor to the edge.
   */
  EdgeAccessor insert_edge(DbTransaction& db_trans, VertexAccessor& from,
                           VertexAccessor& to, EdgeType type);


  /** transaction engine related to this database */
  tx::Engine tx_engine;


  /** garbage collector related to this database*/
// TODO bring back garbage collection
//  Garbage garbage = {tx_engine};

// TODO bring back shapshot engine
//  SnapshotEngine snap_engine = {*this};

  // database name
  const std::string name_;

private:
  // main storage for the graph
  SkipList<mvcc::VersionList<Edge>*> edges_;
  SkipList<mvcc::VersionList<Vertex>*> vertices_;

  // utility stuff
  const PassKey<GraphDb> pass_key;
};

