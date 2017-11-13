#pragma once

#include <experimental/filesystem>

class GraphDbAccessor;

namespace durability {
using path = std::experimental::filesystem::path;

/** Generates a path for a DB snapshot in the given folder in a well-defined
 * sortable format. */
path MakeSnapshotPath(const path &snapshot_folder);

/**
 * Make snapshot and save it in snapshots folder. Returns true if successful.
 * @param db_accessor:
 *     GraphDbAccessor used to access elements of GraphDb.
 * @param snapshot_folder:
 *     folder where snapshots are stored.
 * @param snapshot_max_retained:
 *     maximum number of snapshots stored in snapshot folder.
 */
bool MakeSnapshot(GraphDbAccessor &db_accessor, const path &snapshot_folder,
                  int snapshot_max_retained);
}
