#pragma once

#include <experimental/filesystem>

class GraphDbAccessor;

namespace durability {
using path = std::experimental::filesystem::path;

/** Generates a path for a DB snapshot in the given folder in a well-defined
 * sortable format. */
// TODO review - move to paths.hpp?
path MakeSnapshotPath(const path &durability_dir);

/**
 * Make snapshot and save it in snapshots folder. Returns true if successful.
 * @param db_accessor- GraphDbAccessor used to access elements of GraphDb.
 * @param durability_dir - directory where durability data is stored.
 * @param snapshot_max_retained - maximum number of snapshots to retain.
 */
bool MakeSnapshot(GraphDbAccessor &db_accessor, const path &durability_dir,
                  int snapshot_max_retained);
}
