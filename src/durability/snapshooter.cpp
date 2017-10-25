#include <algorithm>

#include <glog/logging.h>

#include "durability/snapshooter.hpp"

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/version.hpp"
#include "utils/datetime/timestamp.hpp"

bool Snapshooter::MakeSnapshot(GraphDbAccessor &db_accessor_,
                               const fs::path &snapshot_folder,
                               const int snapshot_max_retained) {
  if (!fs::exists(snapshot_folder) &&
      !fs::create_directories(snapshot_folder)) {
    LOG(ERROR) << "Error while creating directory " << snapshot_folder;
    return false;
  }
  const auto snapshot_file = GetSnapshotFileName(snapshot_folder);
  if (fs::exists(snapshot_file)) return false;
  if (Encode(snapshot_file, db_accessor_)) {
    MaintainMaxRetainedFiles(snapshot_folder, snapshot_max_retained);
    return true;
  }
  return false;
}

bool Snapshooter::Encode(const fs::path &snapshot_file,
                         GraphDbAccessor &db_accessor_) {
  try {
    HashedFileWriter buffer(snapshot_file);
    communication::bolt::BaseEncoder<HashedFileWriter> encoder(buffer);
    int64_t vertex_num = 0, edge_num = 0;

    encoder.WriteRAW(durability::kMagicNumber.data(),
                     durability::kMagicNumber.size());
    encoder.WriteInt(durability::kVersion);

    // Write the transaction snapshot into the snapshot. It's used when
    // recovering from the combination of snapshot and write-ahead-log.
    {
      std::vector<query::TypedValue> tx_snapshot;
      for (int64_t tx : db_accessor_.transaction().snapshot())
        tx_snapshot.emplace_back(tx);
      encoder.WriteList(tx_snapshot);
    }

    // Write label+property indexes as list ["label", "property", ...]
    {
      std::vector<query::TypedValue> index_vec;
      for (const auto &key : db_accessor_.GetIndicesKeys()) {
        index_vec.emplace_back(db_accessor_.LabelName(key.label_));
        index_vec.emplace_back(db_accessor_.PropertyName(key.property_));
      }
      encoder.WriteList(index_vec);
    }

    for (const auto &vertex : db_accessor_.Vertices(false)) {
      encoder.WriteVertex(vertex);
      vertex_num++;
    }
    for (const auto &edge : db_accessor_.Edges(false)) {
      encoder.WriteEdge(edge);
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

fs::path GetSnapshotFileName(const fs::path &snapshot_folder) {
  std::string date_str =
      Timestamp(Timestamp::now())
          .to_string("{:04d}_{:02d}_{:02d}__{:02d}_{:02d}_{:02d}_{:05d}");
  return snapshot_folder / date_str;
}

std::vector<fs::path> Snapshooter::GetSnapshotFiles(
    const fs::path &snapshot_folder) {
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshot_folder))
    files.push_back(file.path());
  return files;
}

void Snapshooter::MaintainMaxRetainedFiles(const fs::path &snapshot_folder,
                                           int snapshot_max_retained) {
  if (snapshot_max_retained == -1) return;
  std::vector<fs::path> files = GetSnapshotFiles(snapshot_folder);
  if (static_cast<int>(files.size()) <= snapshot_max_retained) return;
  sort(files.begin(), files.end());
  for (int i = 0; i < static_cast<int>(files.size()) - snapshot_max_retained;
       ++i) {
    if (!fs::remove(files[i])) {
      LOG(ERROR) << "Error while removing file: " << files[i];
    }
  }
}
