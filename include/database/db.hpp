#pragma once

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

#include "snapshot/snapshot_engine.hpp"
#include "storage/garbage/garbage.hpp"
#include "storage/graph.hpp"
#include "transactions/engine.hpp"

class Indexes;

// TODO: Maybe split this in another layer between Db and Dbms. Where the new
// layer would hold SnapshotEngine and his kind of concept objects. Some
// guidelines would be: retain objects which are necessary to implement querys
// in Db, the rest can be moved to the new layer.

/**
 * Main class which represents Database concept in code.
 */
class Db {
public:
  using sptr = std::shared_ptr<Db>;

  /**
   * This constructor will create a database with the name "default"
   *
   * NOTE: explicit is here to prevent compiler from evaluating const char *
   * into a bool.
   *
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  explicit Db(bool import_snapshot = true);

  /**
   * Construct database with a custom name.
   *
   * @param name database name
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  Db(const char *name, bool import_snapshot = true);

  /**
   * Construct database with a custom name.
   *
   * @param name database name
   * @param import_snapshot will in constructor import latest snapshot
   *                        into the db.
   */
  Db(const std::string &name, bool import_snapshot = true);

  /**
   * Database object can't be copied.
   */
  Db(const Db &db) = delete;

private:
  /** database name */
  const std::string name_;

public:
  /** transaction engine related to this database */
  tx::Engine tx_engine;

  /** graph related to this database */
  Graph graph;

  /** garbage collector related to this database*/
  Garbage garbage = {tx_engine};

  /**
   * snapshot engine related to this database
   *
   * \b IMPORTANT: has to be initialized after name
   * */
  SnapshotEngine snap_engine = {*this};

  /**
   * Creates Indexes for this database.
   */
  Indexes indexes();
  // TODO: Indexes should be created only once somwhere Like Db or layer
  // between Db and Dbms.

  /**
   * Returns a name of the database.
   *
   * @return database name
   */
  std::string const &name() const;
};
