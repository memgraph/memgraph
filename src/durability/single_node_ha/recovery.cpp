#include "durability/single_node_ha/recovery.hpp"

#include <filesystem>
#include <limits>
#include <optional>
#include <unordered_map>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "database/single_node_ha/graph_db_accessor.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/single_node_ha/paths.hpp"
#include "durability/single_node_ha/version.hpp"
#include "glue/communication.hpp"
#include "storage/single_node_ha/indexes/label_property_index.hpp"
#include "transactions/type.hpp"
#include "utils/algorithm.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;

namespace durability {

using communication::bolt::Value;
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash) {
  auto pos = buffer.Tellg();
  auto offset = sizeof(vertex_count) + sizeof(edge_count) + sizeof(hash);
  buffer.Seek(-offset, std::ios_base::end);
  bool r_val = buffer.ReadType(vertex_count, false) &&
               buffer.ReadType(edge_count, false) &&
               buffer.ReadType(hash, false);
  buffer.Seek(pos);
  return r_val;
}

namespace {
using communication::bolt::Value;

#define RETURN_IF_NOT(condition) \
  if (!(condition)) {            \
    reader.Close();              \
    return false;                \
  }

bool RecoverSnapshot(const fs::path &snapshot_file, database::GraphDb *db,
                     RecoveryData *recovery_data) {
  HashedFileReader reader;
  communication::bolt::Decoder<HashedFileReader> decoder(reader);

  RETURN_IF_NOT(reader.Open(snapshot_file));

  auto magic_number = durability::kSnapshotMagic;
  reader.Read(magic_number.data(), magic_number.size());
  RETURN_IF_NOT(magic_number == durability::kSnapshotMagic);

  // Read the vertex and edge count, and the hash, from the end of the snapshot.
  int64_t vertex_count;
  int64_t edge_count;
  uint64_t hash;
  RETURN_IF_NOT(
      durability::ReadSnapshotSummary(reader, vertex_count, edge_count, hash));

  Value dv;
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int) &&
                dv.ValueInt() == durability::kVersion);

  // A list of label+property indexes.
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::List));
  auto index_value = dv.ValueList();
  for (auto it = index_value.begin(); it != index_value.end();) {
    auto label = *it++;
    RETURN_IF_NOT(it != index_value.end());
    auto property = *it++;
    RETURN_IF_NOT(it != index_value.end());
    auto unique = *it++;
    RETURN_IF_NOT(label.IsString() && property.IsString() && unique.IsBool());
    recovery_data->indexes.emplace_back(
        IndexRecoveryData{label.ValueString(), property.ValueString(),
                          /*create = */ true, unique.ValueBool()});
  }

  auto dba = db->Access();
  std::unordered_map<uint64_t, VertexAccessor> vertices;
  for (int64_t i = 0; i < vertex_count; ++i) {
    Value vertex_dv;
    RETURN_IF_NOT(decoder.ReadValue(&vertex_dv, Value::Type::Vertex));
    auto &vertex = vertex_dv.ValueVertex();
    auto vertex_accessor = dba.InsertVertex(vertex.id.AsUint());

    for (const auto &label : vertex.labels) {
      vertex_accessor.add_label(dba.Label(label));
    }
    for (const auto &property_pair : vertex.properties) {
      vertex_accessor.PropsSet(dba.Property(property_pair.first),
                               glue::ToPropertyValue(property_pair.second));
    }
    vertices.insert({vertex.id.AsUint(), vertex_accessor});
  }

  for (int64_t i = 0; i < edge_count; ++i) {
    Value edge_dv;
    RETURN_IF_NOT(decoder.ReadValue(&edge_dv, Value::Type::Edge));
    auto &edge = edge_dv.ValueEdge();
    auto it_from = vertices.find(edge.from.AsUint());
    auto it_to = vertices.find(edge.to.AsUint());
    RETURN_IF_NOT(it_from != vertices.end() && it_to != vertices.end());
    auto edge_accessor =
        dba.InsertEdge(it_from->second, it_to->second,
                        dba.EdgeType(edge.type), edge.id.AsUint());

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(dba.Property(property_pair.first),
                             glue::ToPropertyValue(property_pair.second));
  }

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  reader.ReadType(vertex_count);
  reader.ReadType(edge_count);
  if (!reader.Close() || reader.hash() != hash) {
    dba.Abort();
    return false;
  }

  dba.Commit();
  return true;
}

#undef RETURN_IF_NOT

}  // anonymous namespace

bool RecoverSnapshot(database::GraphDb *db, RecoveryData *recovery_data,
                     const fs::path &durability_dir,
                     const std::string &snapshot_filename) {
  const auto snapshot_dir = durability_dir / kSnapshotDir;
  if (!fs::exists(snapshot_dir) || !fs::is_directory(snapshot_dir)) {
    LOG(WARNING) << "Missing snapshot directory!";
    return false;
  }

  const auto snapshot = snapshot_dir / snapshot_filename;
  if (!fs::exists(snapshot)) {
    LOG(WARNING) << "Missing snapshot file!";
    return false;
  }

  LOG(INFO) << "Starting snapshot recovery from: " << snapshot;
  if (!RecoverSnapshot(snapshot, db, recovery_data)) {
    LOG(WARNING) << "Snapshot recovery failed.";
    return false;
  }

  LOG(INFO) << "Snapshot recovery successful.";
  return true;
}

void RecoverIndexes(database::GraphDb *db,
                    const std::vector<IndexRecoveryData> &indexes) {
  auto dba = db->Access();
  for (const auto &index : indexes) {
    auto label = dba.Label(index.label);
    auto property = dba.Property(index.property);
    if (index.create) {
      dba.BuildIndex(label, property, index.unique);
    } else {
      dba.DeleteIndex(label, property);
    }
  }
  dba.Commit();
}

}  // namespace durability
