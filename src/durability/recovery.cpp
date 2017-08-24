#include "durability/recovery.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/file_reader_buffer.hpp"
#include "query/typed_value.hpp"

using communication::bolt::DecodedValue;

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

bool Recovery::Decode(const fs::path &snapshot_file,
                      GraphDbAccessor &db_accessor) {
  FileReaderBuffer buffer;
  communication::bolt::Decoder<FileReaderBuffer> decoder(buffer);

  snapshot::Summary summary;
  if (!buffer.Open(snapshot_file, summary)) {
    buffer.Close();
    return false;
  }
  std::unordered_map<uint64_t, VertexAccessor> vertices;

  DecodedValue dv;
  if (!decoder.ReadValue(&dv, DecodedValue::Type::List)) {
    buffer.Close();
    return false;
  }
  auto &label_property_vector = dv.ValueList();
  for (int i = 0; i < label_property_vector.size(); i += 2) {
    auto label = label_property_vector[i].ValueString();
    auto property = label_property_vector[i + 1].ValueString();
    db_accessor.BuildIndex(db_accessor.Label(label),
                           db_accessor.Property(property));
  }

  for (int64_t i = 0; i < summary.vertex_num_; ++i) {
    DecodedValue vertex_dv;
    if (!decoder.ReadValue(&vertex_dv, DecodedValue::Type::Vertex)) {
      buffer.Close();
      return false;
    }
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
  for (int64_t i = 0; i < summary.edge_num_; ++i) {
    DecodedValue edge_dv;
    if (!decoder.ReadValue(&edge_dv, DecodedValue::Type::Edge)) {
      buffer.Close();
      return false;
    }
    auto &edge = edge_dv.ValueEdge();
    auto it_from = vertices.find(edge.from);
    auto it_to = vertices.find(edge.to);
    if (it_from == vertices.end() || it_to == vertices.end()) {
      buffer.Close();
      return false;
    }
    auto edge_accessor = db_accessor.InsertEdge(
        it_from->second, it_to->second, db_accessor.EdgeType(edge.type));

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(db_accessor.Property(property_pair.first),
                             query::TypedValue(property_pair.second));
  }

  uint64_t hash = buffer.hash();
  if (!buffer.Close()) return false;
  return hash == summary.hash_;
}
