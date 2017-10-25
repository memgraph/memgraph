#include "durability/recovery.hpp"

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/version.hpp"
#include "query/typed_value.hpp"
#include "utils/string.hpp"

using communication::bolt::DecodedValue;

namespace durability {
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
}

bool Recovery::Recover(const fs::path &snapshot_file,
                       GraphDbAccessor &db_accessor) {
  if (!fs::exists(snapshot_file)) return false;
  if (!Decode(snapshot_file, db_accessor)) {
    db_accessor.Abort();
    return false;
  }
  db_accessor.Commit();
  return true;
}

#define RETURN_IF_NOT(condition) \
  if (!(condition)) {            \
    buffer.Close();              \
    return false;                \
  }

bool Recovery::Decode(const fs::path &snapshot_file,
                      GraphDbAccessor &db_accessor) {
  HashedFileReader buffer;
  communication::bolt::Decoder<HashedFileReader> decoder(buffer);

  RETURN_IF_NOT(buffer.Open(snapshot_file));
  std::unordered_map<uint64_t, VertexAccessor> vertices;

  auto magic_number = durability::kMagicNumber;
  buffer.Read(magic_number.data(), magic_number.size());
  RETURN_IF_NOT(magic_number == durability::kMagicNumber);

  // Read the vertex and edge count, and the hash, from the end of the snapshot.
  int64_t vertex_count;
  int64_t edge_count;
  uint64_t hash;
  RETURN_IF_NOT(
      durability::ReadSnapshotSummary(buffer, vertex_count, edge_count, hash));

  DecodedValue dv;

  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::Int) &&
                dv.ValueInt() == durability::kVersion);

  // Transaction snapshot of the transaction that created the snapshot :D In the
  // current recovery implementation it's ignored.
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::List));

  // A list of label+property indexes.
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::List));
  auto indexes = dv.ValueList();
  for (auto it = indexes.begin(); it != indexes.end();) {
    auto label = *it++;
    RETURN_IF_NOT(it != indexes.end());
    auto property = *it++;
    RETURN_IF_NOT(label.IsString() && property.IsString());
    db_accessor.BuildIndex(db_accessor.Label(label.ValueString()),
                           db_accessor.Property(property.ValueString()));
  }

  for (int64_t i = 0; i < vertex_count; ++i) {
    DecodedValue vertex_dv;
    RETURN_IF_NOT(decoder.ReadValue(&vertex_dv, DecodedValue::Type::Vertex));
    auto &vertex = vertex_dv.ValueVertex();
    auto vertex_accessor = db_accessor.InsertVertex();
    for (const auto &label : vertex.labels) {
      vertex_accessor.add_label(db_accessor.Label(label));
    }
    for (const auto &property_pair : vertex.properties) {
      vertex_accessor.PropsSet(db_accessor.Property(property_pair.first),
                               query::TypedValue(property_pair.second));
    }
    vertices.insert({vertex.id, vertex_accessor});
  }
  for (int64_t i = 0; i < edge_count; ++i) {
    DecodedValue edge_dv;
    RETURN_IF_NOT(decoder.ReadValue(&edge_dv, DecodedValue::Type::Edge));
    auto &edge = edge_dv.ValueEdge();
    auto it_from = vertices.find(edge.from);
    auto it_to = vertices.find(edge.to);
    RETURN_IF_NOT(it_from != vertices.end() && it_to != vertices.end());
    auto edge_accessor = db_accessor.InsertEdge(
        it_from->second, it_to->second, db_accessor.EdgeType(edge.type));

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(db_accessor.Property(property_pair.first),
                             query::TypedValue(property_pair.second));
  }

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  buffer.ReadType(vertex_count);
  buffer.ReadType(edge_count);

  return (buffer.Close() && buffer.hash() == hash);
}

#undef RETURN_IF_NOT
