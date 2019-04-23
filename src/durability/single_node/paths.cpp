#include "durability/single_node/paths.hpp"

#include <filesystem>
#include <optional>
#include <string>

#include "glog/logging.h"

#include "transactions/type.hpp"
#include "utils/string.hpp"
#include "utils/timestamp.hpp"

namespace durability {

namespace fs = std::filesystem;

// This is the prefix used for WAL and Snapshot filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat =
    "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

// TODO: This shouldn't be used to get the transaction ID from a WAL file,
// instead the file should be parsed and the transaction ID should be read from
// the file.
std::optional<tx::TransactionId> TransactionIdFromWalFilename(
    const std::string &name) {
  if (utils::EndsWith(name, "current"))
    return std::numeric_limits<tx::TransactionId>::max();
  // Get the max_transaction_id from the file name that has format
  // "XXXXX_tx_<MAX_TRANS_ID>"
  auto file_name_split = utils::RSplit(name, "_", 1);
  if (file_name_split.size() != 2) {
    LOG(WARNING) << "Unable to parse WAL file name: " << name;
    return std::nullopt;
  }
  auto &tx_id_str = file_name_split[1];
  try {
    return std::stoll(tx_id_str);
  } catch (std::invalid_argument &) {
    LOG(WARNING) << "Unable to parse WAL file name tx ID: " << tx_id_str;
    return std::nullopt;
  } catch (std::out_of_range &) {
    LOG(WARNING) << "WAL file name tx ID too large: " << tx_id_str;
    return std::nullopt;
  }
}

/// Generates a file path for a write-ahead log file. If given a transaction ID
/// the file name will contain it. Otherwise the file path is for the "current"
/// WAL file for which the max tx id is still unknown.
fs::path WalFilenameForTransactionId(const std::filesystem::path &wal_dir,
                                     std::optional<tx::TransactionId> tx_id) {
  auto file_name = utils::Timestamp::Now().ToString(kTimestampFormat);
  if (tx_id) {
    file_name += "_tx_" + std::to_string(*tx_id);
  } else {
    file_name += "_current";
  }
  return wal_dir / file_name;
}

fs::path MakeSnapshotPath(const fs::path &durability_dir,
                          tx::TransactionId tx_id) {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  auto file_name = date_str + "_tx_" + std::to_string(tx_id);
  return durability_dir / kSnapshotDir / file_name;
}

// TODO: This shouldn't be used to get the transaction ID from a snapshot file,
// instead the file should be parsed and the transaction ID should be read from
// the file.
std::optional<tx::TransactionId> TransactionIdFromSnapshotFilename(
    const std::string &name) {
  auto file_name_split = utils::RSplit(name, "_tx_", 1);
  if (file_name_split.size() != 2) {
    LOG(WARNING) << "Unable to parse snapshot file name: " << name;
    return std::nullopt;
  }
  try {
    return std::stoll(file_name_split[1]);
  } catch (std::invalid_argument &) {
    LOG(WARNING) << "Unable to parse snapshot file name tx ID: "
                 << file_name_split[1];
    return std::nullopt;
  } catch (std::out_of_range &) {
    LOG(WARNING) << "Unable to parse snapshot file name tx ID: "
                 << file_name_split[1];
    return std::nullopt;
  }
}
}  // namespace durability
