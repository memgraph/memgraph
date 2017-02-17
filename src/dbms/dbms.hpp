#pragma once

#include "config/config.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"

//#include "dbms/cleaner.hpp"
//#include "snapshot/snapshoter.hpp"

class Dbms
{
public:
    Dbms() {
      // create the default database and set is a active
      active("default");
    }

    /**
     * Returns an accessor to the active database.
     */
    GraphDbAccessor active();

    /**
     * Set the database with the given name to be active.
     * If there is no database with the given name,
     * it's created.
     *
     * @return an accessor to the database with the given name.
     */
    GraphDbAccessor active(const std::string &name);

    // TODO: DELETE action

private:
    // dbs container
    ConcurrentMap<std::string, GraphDb> dbs;

    // currently active database
    std::atomic<GraphDb *> active_db;

//    // Cleaning thread.
//      TODO re-enable cleaning
//    Cleaning cleaning = {dbs, CONFIG_INTEGER(config::CLEANING_CYCLE_SEC)};
//
//    // Snapshoting thread.
//      TODO re-enable cleaning
//    Snapshoter snapshoter = {dbs, CONFIG_INTEGER(config::SNAPSHOT_CYCLE_SEC)};
};
