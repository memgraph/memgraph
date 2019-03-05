#include <experimental/filesystem>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/single_node/recovery.hpp"
#include "durability/single_node/version.hpp"

DEFINE_string(snapshot_file, "", "Snapshot file location");

using communication::bolt::Value;
namespace fs = std::experimental::filesystem;

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // This makes sure we update the explorer when we bump the snapshot version.
  // Snapshot layout is described in durability/version.hpp
  static_assert(durability::kVersion == 8,
                "Wrong snapshot version, please update!");

  fs::path snapshot_path(FLAGS_snapshot_file);
  CHECK(fs::exists(snapshot_path)) << "File doesn't exist!";

  HashedFileReader reader;
  communication::bolt::Decoder<HashedFileReader> decoder(reader);

  CHECK(reader.Open(snapshot_path)) << "Couldn't open snapshot file!";

  auto magic_number = durability::kSnapshotMagic;
  reader.Read(magic_number.data(), magic_number.size());
  CHECK(magic_number == durability::kSnapshotMagic) << "Magic number mismatch";

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

  decoder.ReadValue(&dv, Value::Type::List);
  auto existence_constraint = dv.ValueList();
  for (auto it = existence_constraint.begin(); it != existence_constraint.end();) {
    std::string log("Adding existence constraint: ");
    CHECK(it->IsString()) << "Label is not a string!";
    log.append(it->ValueString());
    log.append(" -> [");
    ++it;
    CHECK(it->IsInt()) << "Number of properties is not an int!";
    int64_t prop_size = it->ValueInt();
    ++it;
    for (size_t i = 0; i < prop_size; ++i) {
      CHECK(it->IsString()) << "Property is not a string!";
      log.append(it->ValueString());
      if (i != prop_size -1) {
        log.append(", ");
      } else {
        log.append("]");
      }

      ++it;
    }

    LOG(INFO) << log;
  }

  for (int64_t i = 0; i < vertex_count; ++i) {
    auto vertex = decoder.ReadValue(&dv, Value::Type::Vertex);
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
