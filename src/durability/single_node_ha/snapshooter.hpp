#pragma once

#include <experimental/filesystem>

#include "database/single_node_ha/graph_db.hpp"

namespace durability {

/// Make snapshot and save it in snapshots folder. Returns true if successful.
/// @param db - database for which we are creating a snapshot
/// @param dba - db accessor with which we are creating a snapshot (reading
///              data)
/// @param durability_dir - directory where durability data is stored.
bool MakeSnapshot(database::GraphDb &db, database::GraphDbAccessor &dba,
                  const std::experimental::filesystem::path &durability_dir);

/// Remove all snapshots inside the snapshot durability directory.
void RemoveAllSnapshots(
    const std::experimental::filesystem::path &durability_dir);

}  // namespace durability
