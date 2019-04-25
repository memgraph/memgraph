#pragma once

#include <filesystem>
#include <string>

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kBackupDir = ".backup";

/// Generates a filename for a DB snapshot in the given folder in a well-defined
/// sortable format with last included term and last included index from which
/// the snapshot is created appended to the file name.
std::string GetSnapshotFilename(uint64_t last_included_term,
                                uint64_t last_included_index);

/// Generates a full path for a DB snapshot.
std::filesystem::path MakeSnapshotPath(
    const std::filesystem::path &durability_dir,
    const std::string &snapshot_filename);
}  // namespace durability
