#include "durability/single_node_ha/paths.hpp"

#include <experimental/filesystem>
#include <experimental/optional>
#include <string>

#include "glog/logging.h"

#include "transactions/type.hpp"
#include "utils/string.hpp"
#include "utils/timestamp.hpp"

namespace durability {

namespace fs = std::experimental::filesystem;

fs::path MakeSnapshotPath(const fs::path &durability_dir,
                          tx::TransactionId tx_id) {
  std::string date_str =
      utils::Timestamp(utils::Timestamp::Now())
          .ToString("{:04d}_{:02d}_{:02d}__{:02d}_{:02d}_{:02d}_{:05d}");
  auto file_name = date_str + "_tx_" + std::to_string(tx_id);
  return durability_dir / kSnapshotDir / file_name;
}

std::experimental::optional<tx::TransactionId>
TransactionIdFromSnapshotFilename(const std::string &name) {
  auto nullopt = std::experimental::nullopt;
  auto file_name_split = utils::RSplit(name, "_tx_", 1);
  if (file_name_split.size() != 2) {
    LOG(WARNING) << "Unable to parse snapshot file name: " << name;
    return nullopt;
  }
  try {
    return std::stoll(file_name_split[1]);
  } catch (std::invalid_argument &) {
    LOG(WARNING) << "Unable to parse snapshot file name tx ID: "
                 << file_name_split[1];
    return nullopt;
  } catch (std::out_of_range &) {
    LOG(WARNING) << "Unable to parse snapshot file name tx ID: "
                 << file_name_split[1];
    return nullopt;
  }
}
}  // namespace durability
