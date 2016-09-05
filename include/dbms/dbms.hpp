#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/db.hpp"
#include "dbms/cleaner.hpp"

class Dbms
{
public:
    Dbms() { create_default(); }

    // returns active database
    Db &active();

    // set active database
    // if active database doesn't exist creates one
    Db &active(const std::string &name);

    // TODO: DELETE action

private:
    // creates default database
    Db &create_default() { return active("default"); }

    // dbs container
    ConcurrentMap<std::string, Db> dbs;

    // currently active database
    std::atomic<Db *> active_db;

    Cleaning cleaning = {dbs};
};
