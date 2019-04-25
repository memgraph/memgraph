#include "durability/single_node_ha/paths.hpp"

#include "utils/string.hpp"
#include "utils/timestamp.hpp"

namespace durability {

namespace fs = std::filesystem;

// This is the prefix used for WAL and Snapshot filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat =
    "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

std::string GetSnapshotFilename(uint64_t last_included_term,
                                uint64_t last_included_index) {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_term_" + std::to_string(last_included_term) + "_index_" +
         std::to_string(last_included_index);
}

fs::path MakeSnapshotPath(const fs::path &durability_dir,
                          const std::string &snapshot_filename) {
  return durability_dir / kSnapshotDir / snapshot_filename;
}
}  // namespace durability
