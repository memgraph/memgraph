#pragma once

#include <experimental/filesystem>
#include <experimental/optional>

#include "transactions/type.hpp"

namespace durability {
const std::string kSnapshotDir = "snapshots";
const std::string kWalDir = "wal";

/// Esures that the given dir either exists or is succsefully created.
bool EnsureDir(const std::experimental::filesystem::path &dir);

/// Ensures the given durability directory exists and is ready for use. Creates
/// the directory if it doesn't exist.
void CheckDurabilityDir(const std::string &durability_dir);

/// Returns the transaction id contained in the file name. If the filename is
/// not a parseable WAL file name, nullopt is returned. If the filename
/// represents the "current" WAL file, then the maximum possible transaction ID
/// is returned because that's appropriate for the recovery logic (the current
/// WAL does not yet have a maximum transaction ID and can't be discarded by
/// the recovery regardless of the snapshot from which the transaction starts).
std::experimental::optional<tx::transaction_id_t> TransactionIdFromWalFilename(
    const std::string &name);

/// Generates a file path for a write-ahead log file. If given a transaction ID
/// the file name will contain it. Otherwise the file path is for the "current"
/// WAL file for which the max tx id is still unknown.
std::experimental::filesystem::path WalFilenameForTransactionId(
    const std::experimental::filesystem::path &wal_dir,
    std::experimental::optional<tx::transaction_id_t> tx_id =
        std::experimental::nullopt);
}  // namespace durability
