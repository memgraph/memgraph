#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "gflags/gflags.h"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/recovery.hpp"
#include "utils/exceptions.hpp"

DECLARE_string(snapshot_directory);
DECLARE_bool(recover_on_startup);

namespace fs = std::experimental::filesystem;

// Always be sure that Dbms object is destructed before main exits, i. e. Dbms
// object shouldn't be part of global/static variable, except if its destructor
// is explicitly called before main exits.
// Consider code:
//
// Dbms dbms;  // KeyIndex is created as a part of dbms.
// int main() {
//   auto dba = dbms.active();
//   auto v = dba->InsertVertex();
//   v.add_label(dba->Label(
//       "Start"));  // New SkipList is created in KeyIndex for LabelIndex.
//                   // That SkipList creates SkipListGc which
//                   // initialises static Executor object.
//   return 0;
// }
//
// After main exits: 1. Executor is destructed, 2. KeyIndex is destructed.
// Destructor of KeyIndex calls delete on created SkipLists which destroy
// SkipListGc that tries to use Excutioner object that doesn't exist anymore.
// -> CRASH
class Dbms {
 public:
  Dbms() {
    auto snapshot_root_dir = fs::path(FLAGS_snapshot_directory);
    if (fs::exists(snapshot_root_dir) && !fs::is_directory(snapshot_root_dir)) {
      throw utils::BasicException("Specified snapshot directory is a file!");
    }

    if (FLAGS_recover_on_startup) {
      if (fs::exists(snapshot_root_dir)) {
        auto accessor = dbs.access();
        for (auto &snapshot_db_dir :
             fs::directory_iterator(FLAGS_snapshot_directory)) {
          // The snapshot folder structure is:
          //   snapshot_root_dir/database_name/[timestamp]
          if (fs::is_directory(snapshot_db_dir)) {
            // Create db and set it active
            active(snapshot_db_dir.path().filename(), snapshot_db_dir);
          }
        }
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
};
