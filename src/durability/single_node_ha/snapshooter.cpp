#include "durability/single_node_ha/snapshooter.hpp"

#include <algorithm>

#include <glog/logging.h>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "database/single_node_ha/graph_db_accessor.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/single_node_ha/paths.hpp"
#include "durability/single_node_ha/version.hpp"
#include "glue/communication.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;

namespace durability {

// Snapshot layout is described in durability/version.hpp
static_assert(durability::kVersion == 9,
              "Wrong snapshot version, please update!");

namespace {
bool Encode(const fs::path &snapshot_file, database::GraphDb &db,
            database::GraphDbAccessor &dba) {
  try {
    HashedFileWriter buffer(snapshot_file);
    communication::bolt::BaseEncoder<HashedFileWriter> encoder(buffer);
    int64_t vertex_num = 0, edge_num = 0;

    encoder.WriteRAW(durability::kSnapshotMagic.data(),
                     durability::kSnapshotMagic.size());
    encoder.WriteInt(durability::kVersion);

    // Write label+property indexes as list ["label", "property", ...]
    {
      std::vector<communication::bolt::Value> index_vec;
      for (const auto &key : dba.GetIndicesKeys()) {
        index_vec.emplace_back(dba.LabelName(key.label_));
        index_vec.emplace_back(dba.PropertyName(key.property_));
      }
      encoder.WriteList(index_vec);
    }

    for (const auto &vertex : dba.Vertices(false)) {
      encoder.WriteVertex(glue::ToBoltVertex(vertex));
      vertex_num++;
    }
    for (const auto &edge : dba.Edges(false)) {
      encoder.WriteEdge(glue::ToBoltEdge(edge));
      edge_num++;
    }
    buffer.WriteValue(vertex_num);
    buffer.WriteValue(edge_num);
    buffer.WriteValue(buffer.hash());
    buffer.Close();
  } catch (const std::ifstream::failure &) {
    if (fs::exists(snapshot_file) && !fs::remove(snapshot_file)) {
      LOG(ERROR) << "Error while removing corrupted snapshot file: "
                 << snapshot_file;
    }
    return false;
  }
  return true;
}

/// Remove old snapshots but leave at most `keep`  number of latest ones.
void RemoveOldSnapshots(const fs::path &snapshot_dir, uint16_t keep) {
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshot_dir))
    files.push_back(file.path());
  if (static_cast<uint16_t>(files.size()) <= keep) return;
  sort(files.begin(), files.end());
  for (int i = 0; i < static_cast<uint16_t>(files.size()) - keep; ++i) {
    if (!fs::remove(files[i])) {
      LOG(ERROR) << "Error while removing file: " << files[i];
    }
  }
}

}  // namespace

bool MakeSnapshot(database::GraphDb &db, database::GraphDbAccessor &dba,
                  const fs::path &durability_dir,
                  const std::string &snapshot_filename) {
  if (!utils::EnsureDir(durability_dir / kSnapshotDir)) return false;
  const auto snapshot_file =
      MakeSnapshotPath(durability_dir, snapshot_filename);
  if (fs::exists(snapshot_file)) return false;
  if (Encode(snapshot_file, db, dba)) {
    // Only keep the latest snapshot.
    RemoveOldSnapshots(durability_dir / kSnapshotDir, 1);
    return true;
  } else {
    std::error_code error_code;  // Just for exception suppression.
    fs::remove(snapshot_file, error_code);
    return false;
  }
}

void RemoveAllSnapshots(const fs::path &durability_dir) {
  auto snapshot_dir = durability_dir / kSnapshotDir;
  if (!utils::EnsureDir(snapshot_dir)) return;
  RemoveOldSnapshots(snapshot_dir, 0);
}

}  // namespace durability
