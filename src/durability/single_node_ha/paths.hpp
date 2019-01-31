#pragma once

#include <experimental/filesystem>
#include <experimental/optional>

#include "transactions/type.hpp"

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kBackupDir = ".backup";

/// Generates a filename for a DB snapshot in the given folder in a well-defined
/// sortable format with transaction from which the snapshot is created appended
/// to the file name.
std::string GetSnapshotFilename(tx::TransactionId tx_id);

/// Generates a full path for a DB snapshot.
std::experimental::filesystem::path MakeSnapshotPath(
    const std::experimental::filesystem::path &durability_dir,
    const std::string &snapshot_filename);

/// Returns the transaction id contained in the file name. If the filename is
/// not a parseable snapshot file name, nullopt is returned.
std::experimental::optional<tx::TransactionId>
TransactionIdFromSnapshotFilename(const std::string &name);
}  // namespace durability
