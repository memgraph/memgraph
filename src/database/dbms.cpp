#include "gflags/gflags.h"

#include "database/dbms.hpp"

DEFINE_string(snapshot_directory, "snapshots",
              "Relative path to directory in which to save snapshots.");
DEFINE_bool(recover_on_startup, false, "Recover database on startup.");

std::unique_ptr<GraphDbAccessor> Dbms::active() {
  return std::make_unique<GraphDbAccessor>(
      *active_db.load(std::memory_order_acquire));
}

std::unique_ptr<GraphDbAccessor> Dbms::active(const std::string &name,
                                              const fs::path &snapshot_db_dir) {
  auto acc = dbs.access();
  // create db if it doesn't exist
  auto it = acc.find(name);
  if (it == acc.end()) {
    it = acc.emplace(name, std::forward_as_tuple(name),
                     std::forward_as_tuple(name, snapshot_db_dir))
             .first;
  }

  // set and return active db
  auto &db = it->second;
  active_db.store(&db);
  return std::make_unique<GraphDbAccessor>(db);
}
