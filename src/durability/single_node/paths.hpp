#pragma once

#include <filesystem>
#include <optional>

#include "transactions/type.hpp"

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kWalDir = "wal";
const std::string kBackupDir = ".backup";

/// Returns the transaction id contained in the file name. If the filename is
/// not a parseable WAL file name, nullopt is returned. If the filename
/// represents the "current" WAL file, then the maximum possible transaction ID
/// is returned because that's appropriate for the recovery logic (the current
/// WAL does not yet have a maximum transaction ID and can't be discarded by
/// the recovery regardless of the snapshot from which the transaction starts).
std::optional<tx::TransactionId> TransactionIdFromWalFilename(
    const std::string &name);

/// Generates a file path for a write-ahead log file. If given a transaction ID
/// the file name will contain it. Otherwise the file path is for the "current"
/// WAL file for which the max tx id is still unknown.
std::filesystem::path WalFilenameForTransactionId(
    const std::filesystem::path &wal_dir,
    std::optional<tx::TransactionId> tx_id = std::nullopt);

/// Generates a path for a DB snapshot in the given folder in a well-defined
/// sortable format with transaction from which the snapshot is created appended
/// to the file name.
std::filesystem::path MakeSnapshotPath(
    const std::filesystem::path &durability_dir, tx::TransactionId tx_id);

/// Returns the transaction id contained in the file name. If the filename is
/// not a parseable WAL file name, nullopt is returned.
std::optional<tx::TransactionId> TransactionIdFromSnapshotFilename(
    const std::string &name);
}  // namespace durability
