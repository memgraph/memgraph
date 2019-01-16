#pragma once

#include <experimental/filesystem>
#include <experimental/optional>

#include "transactions/type.hpp"

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kBackupDir = ".backup";

/// Generates a path for a DB snapshot in the given folder in a well-defined
/// sortable format with transaction from which the snapshot is created appended
/// to the file name.
std::experimental::filesystem::path MakeSnapshotPath(
    const std::experimental::filesystem::path &durability_dir,
    tx::TransactionId tx_id);

/// Returns the transaction id contained in the file name. If the filename is
/// not a parseable snapshot file name, nullopt is returned.
std::experimental::optional<tx::TransactionId>
TransactionIdFromSnapshotFilename(const std::string &name);
}  // namespace durability
