#include "dbms/dbms.hpp"

std::unique_ptr<GraphDbAccessor> Dbms::active() {
  return std::make_unique<GraphDbAccessor>(
      *active_db.load(std::memory_order_acquire));
}

std::unique_ptr<GraphDbAccessor> Dbms::active(const std::string &name) {
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
  return std::make_unique<GraphDbAccessor>(db);
}
