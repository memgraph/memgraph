#include "dbms/dbms.hpp"

// returns active database
Db &Dbms::active()
{
    Db *active = active_db.load(std::memory_order_acquire);
    if (UNLIKELY(active == nullptr)) {
        return create_default();
    } else {
        return *active;
    }
}

// set active database
// if active database doesn't exist create one
Db &Dbms::active(const std::string &name)
{
    auto acc = dbs.access();
    // create db if it doesn't exist
    auto it = acc.find(name);
    if (it == acc.end()) {
        Snapshoter &snap = snapshoter;
        it = acc.emplace(name, std::forward_as_tuple(name),
                         std::forward_as_tuple(name))
                 .first;
    }

    // set and return active db
    auto &db = it->second;
    active_db.store(&db, std::memory_order_release);
    return db;
}
