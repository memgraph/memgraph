#pragma once

#include <map>

#include "database/db.hpp"

class Dbms
{
public:
    Dbms() { create_default(); }

    // returns active database
    Db &active()
    {
        if (UNLIKELY(active_db == nullptr)) create_default();

        return *active_db;
    }

    // set active database
    // if active database doesn't exist create one
    Db &active(const std::string &name)
    {
        // create db if it doesn't exist
        if (dbs.find(name) == dbs.end()) {
            dbs.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                        std::forward_as_tuple(name));
        }

        // set and return active db
        auto &db = dbs.at(name);
        return active_db = &db, *active_db;
    }

    // TODO: DELETE action

private:
    // dbs container
    std::map<std::string, Db> dbs;

    // currently active database
    Db *active_db;

    // creates default database
    void create_default() { active("default"); }
};
