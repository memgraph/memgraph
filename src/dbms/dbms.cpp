#include "dbms/dbms.hpp"

// returns active database
GraphDb &Dbms::active()
{
    GraphDb *active = active_db.load(std::memory_order_acquire);
    if (UNLIKELY(active == nullptr)) {
        // There is no active database.
        return create_default();
    } else {
        return *active;
    }
}

// set active database
// if active database doesn't exist create one
GraphDb &Dbms::active(const std::string &name)
{
    auto acc = dbs.access();
    // create db if it doesn't exist
    auto it = acc.find(name);
    if (it == acc.end()) {
        it = acc.emplace(name, std::forward_as_tuple(name),
                         std::forward_as_tuple(name))
                 .first;
    }

    // set and return active db
    auto &db = it->second;
    active_db.store(&db, std::memory_order_release);
    return db;
}
