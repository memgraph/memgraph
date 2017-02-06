#pragma once

#include "config/config.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
//#include "dbms/cleaner.hpp"
//#include "snapshot/snapshoter.hpp"

class Dbms
{
public:
    Dbms() { create_default(); }

    // returns active database
    GraphDb &active();

    // set active database
    // if active database doesn't exist creates one
    GraphDb &active(const std::string &name);

    // TODO: DELETE action

private:
    // creates default database
    GraphDb &create_default() { return active("default"); }

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
