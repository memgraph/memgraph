#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "config/config.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/recovery.hpp"

//#include "dbms/cleaner.hpp"

namespace fs = std::experimental::filesystem;
const std::string DEFAULT_SNAPSHOT_FOLDER = "snapshots";

class Dbms {
 public:
  Dbms() {
    if (CONFIG_BOOL(config::RECOVERY)) {
      auto accessor = dbs.access();
      std::string snapshot_folder = CONFIG(config::SNAPSHOTS_PATH);
      if (snapshot_folder.empty()) snapshot_folder = DEFAULT_SNAPSHOT_FOLDER;
      for (auto &snapshot_db : fs::directory_iterator(snapshot_folder)) {
        // create db and set it active
        active(snapshot_db.path().filename(), snapshot_db);
      }
    }

    // create the default database and set is a active
    active("default");
  }

  /**
   * Returns an accessor to the active database.
   */
  std::unique_ptr<GraphDbAccessor> active();

  /**
   * Set the database with the given name to be active.
   * If there is no database with the given name,
   * it's created. If snapshooting is true, snapshooter starts
   * snapshooting on database creation.
   *
   * @return an accessor to the database with the given name.
   */
  std::unique_ptr<GraphDbAccessor> active(
      const std::string &name, const fs::path &snapshot_db_dir = fs::path());

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
  //    Snapshoter snapshoter = {dbs,
  //    CONFIG_INTEGER(config::SNAPSHOT_CYCLE_SEC)};
};
