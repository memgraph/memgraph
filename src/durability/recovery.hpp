#pragma once

#include <experimental/filesystem>
#include <unordered_map>
#include "database/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"

namespace fs = std::experimental::filesystem;

/**
 * Class used to recover database from snapshot file.
 */
class Recovery {
 public:
  /**
   * Recovers database from snapshot_file. Graph elements are inserted
   * in graph using db_accessor. If recovering fails, false is returned and
   * db_accessor aborts transaction, else true is returned and transaction is
   * commited.
   * @param snapshot_file:
   *    path to snapshot file
   * @param db_accessor:
   *    GraphDbAccessor used to access database.
   */
  bool Recover(const fs::path &snapshot_file, GraphDbAccessor &db_accessor);

 private:
  /**
   * Decodes database from snapshot_file. Graph emlements are inserted in
   * graph using db_accessor. If decoding fails, false is returned, else ture.
   */
  bool Decode(const fs::path &snapshot_file, GraphDbAccessor &db_accessor);
};
