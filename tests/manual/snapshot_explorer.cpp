#include <experimental/filesystem>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "durability/hashed_file_reader.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshot_decoder.hpp"
#include "durability/snapshot_value.hpp"
#include "durability/version.hpp"

DEFINE_string(snapshot_file, "", "Snapshot file location");

using communication::bolt::Value;
namespace fs = std::experimental::filesystem;

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // At the time this was written, the version was 6. This makes sure we update
  // the explorer when we bump the snapshot version.
  static_assert(durability::kVersion == 6,
                "Wrong snapshot version, please update!");

  fs::path snapshot_path(FLAGS_snapshot_file);
  CHECK(fs::exists(snapshot_path)) << "File doesn't exist!";

  HashedFileReader reader;
  durability::SnapshotDecoder<HashedFileReader> decoder(reader);

  CHECK(reader.Open(snapshot_path)) << "Couldn't open snapshot file!";

  auto magic_number = durability::kMagicNumber;
  reader.Read(magic_number.data(), magic_number.size());
  CHECK(magic_number == durability::kMagicNumber) << "Magic number mismatch";

  int64_t vertex_count, edge_count;
  uint64_t hash;

  CHECK(durability::ReadSnapshotSummary(reader, vertex_count, edge_count, hash))
      << "ReadSnapshotSummary failed";

  LOG(INFO) << "Vertex count: " << vertex_count;
  LOG(INFO) << "Edge count: " << edge_count;
  LOG(INFO) << "Hash: " << hash;

  Value dv;

  decoder.ReadValue(&dv, Value::Type::Int);
  CHECK(dv.ValueInt() == durability::kVersion)
      << "Snapshot version mismatch"
      << ", got " << dv.ValueInt() << " expected " << durability::kVersion;

  decoder.ReadValue(&dv, Value::Type::Int);
  LOG(INFO) << "Snapshot was generated for worker id: " << dv.ValueInt();

  decoder.ReadValue(&dv, Value::Type::Int);
  LOG(INFO) << "Vertex generator last id: " << dv.ValueInt();

  decoder.ReadValue(&dv, Value::Type::Int);
  LOG(INFO) << "Edge generator last id: " << dv.ValueInt();

  decoder.ReadValue(&dv, Value::Type::Int);
  LOG(INFO) << "Transactional ID of the snapshooter " << dv.ValueInt();

  decoder.ReadValue(&dv, Value::Type::List);
  for (const auto &value : dv.ValueList()) {
    CHECK(value.IsInt()) << "Transaction is not a number!";
    LOG(INFO) << "Transactional snapshot of the snapshooter "
              << value.ValueInt();
  }

  decoder.ReadValue(&dv, Value::Type::List);

  auto index_value = dv.ValueList();
  for (auto it = index_value.begin(); it != index_value.end();) {
    auto label = *it++;
    CHECK(label.IsString()) << "Label is not a string!";
    CHECK(it != index_value.end()) << "Missing propery for label "
                                   << label.ValueString();
    auto property = *it++;
    CHECK(property.IsString()) << "Property is not a string!";
    LOG(INFO) << "Adding label " << label.ValueString() << " and property "
              << property.ValueString();
  }

  for (int64_t i = 0; i < vertex_count; ++i) {
    auto vertex = decoder.ReadSnapshotVertex();
    CHECK(vertex) << "Failed to read vertex " << i;
  }

  for (int64_t i = 0; i < edge_count; ++i) {
    auto edge = decoder.ReadValue(&dv, Value::Type::Edge);
    CHECK(edge) << "Failed to read edge " << i;
  }

  reader.ReadType(vertex_count);
  LOG(INFO) << "Vertex count: " << vertex_count;

  reader.ReadType(edge_count);
  LOG(INFO) << "Edge count:" << edge_count;

  LOG(INFO) << "Hash: " << reader.hash();

  CHECK(reader.Close()) << "Failed to close the reader";
  return 0;
}
