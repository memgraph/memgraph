#include <algorithm>

#include <glog/logging.h>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/file_writer_buffer.hpp"
#include "utils/datetime/timestamp.hpp"

#include "durability/snapshooter.hpp"

bool Snapshooter::MakeSnapshot(GraphDbAccessor &db_accessor_,
                               const fs::path &snapshot_folder,
                               const int max_retained_snapshots) {
  if (!fs::exists(snapshot_folder) &&
      !fs::create_directories(snapshot_folder)) {
    LOG(ERROR) << "Error while creating directory " << snapshot_folder;
    return false;
  }
  const auto snapshot_file = GetSnapshotFileName(snapshot_folder);
  if (fs::exists(snapshot_file)) return false;
  if (Encode(snapshot_file, db_accessor_)) {
    MaintainMaxRetainedFiles(snapshot_folder, max_retained_snapshots);
    return true;
  }
  return false;
}

bool Snapshooter::Encode(const fs::path &snapshot_file,
                         GraphDbAccessor &db_accessor_) {
  try {
    FileWriterBuffer buffer;
    // BaseEncoder encodes graph elements.
    communication::bolt::BaseEncoder<FileWriterBuffer> encoder(buffer);
    int64_t vertex_num = 0, edge_num = 0;
    buffer.Open(snapshot_file);

    std::vector<query::TypedValue> label_property_vector;
    for (const auto &key : db_accessor_.GetIndicesKeys()) {
      query::TypedValue label(*key.label_);
      query::TypedValue property(*key.property_);
      label_property_vector.push_back(label);
      label_property_vector.push_back(property);
    }
    encoder.WriteList(label_property_vector);

    for (const auto &vertex : db_accessor_.vertices(false)) {
      encoder.WriteVertex(vertex);
      vertex_num++;
    }
    for (const auto &edge : db_accessor_.edges(false)) {
      encoder.WriteEdge(edge);
      edge_num++;
    }
    buffer.WriteSummary(vertex_num, edge_num);
    buffer.Close();
  } catch (std::ifstream::failure e) {
    if (fs::exists(snapshot_file) && !fs::remove(snapshot_file)) {
      LOG(ERROR) << "Error while removing corrupted snapshot file: "
                 << snapshot_file;
    }
    return false;
  }
  return true;
}

fs::path Snapshooter::GetSnapshotFileName(const fs::path &snapshot_folder) {
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
                                           int max_retained_snapshots) {
  if (max_retained_snapshots == -1) return;
  std::vector<fs::path> files = GetSnapshotFiles(snapshot_folder);
  if (static_cast<int>(files.size()) <= max_retained_snapshots) return;
  sort(files.begin(), files.end());
  for (int i = 0; i < static_cast<int>(files.size()) - max_retained_snapshots;
       ++i) {
    if (!fs::remove(files[i])) {
      LOG(ERROR) << "Error while removing file: " << files[i];
    }
  }
}
