#include "durability/recovery.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/file_reader_buffer.hpp"

bool Recovery::Recover(const fs::path &snapshot_file,
                       GraphDbAccessor &db_accessor) {
  if (!fs::exists(snapshot_file)) return false;
  if (!Decode(snapshot_file, db_accessor)) {
    db_accessor.abort();
    return false;
  }
  db_accessor.commit();
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

  query::TypedValue tv;
  if (!decoder.ReadTypedValue(&tv, query::TypedValue::Type::List)) {
    buffer.Close();
    return false;
  }
  auto &label_property_vector = tv.Value<std::vector<query::TypedValue>>();
  for (int i = 0; i < label_property_vector.size(); i += 2) {
    auto label = label_property_vector[i].Value<std::string>();
    auto property = label_property_vector[i + 1].Value<std::string>();
    db_accessor.BuildIndex(db_accessor.label(label),
                           db_accessor.property(property));
  }

  for (int64_t i = 0; i < summary.vertex_num_; ++i) {
    communication::bolt::DecodedVertex vertex;
    if (!decoder.ReadVertex(&vertex)) {
      buffer.Close();
      return false;
    }
    auto vertex_accessor = db_accessor.insert_vertex();
    for (const auto &label : vertex.labels) {
      vertex_accessor.add_label(db_accessor.label(label));
    }
    for (const auto &property_pair : vertex.properties) {
      vertex_accessor.PropsSet(db_accessor.property(property_pair.first),
                               property_pair.second);
    }
    vertices.insert({vertex.id, vertex_accessor});
  }
  for (int64_t i = 0; i < summary.edge_num_; ++i) {
    communication::bolt::DecodedEdge edge;
    if (!decoder.ReadEdge(&edge)) {
      buffer.Close();
      return false;
    }
    auto it_from = vertices.find(edge.from);
    auto it_to = vertices.find(edge.to);
    if (it_from == vertices.end() || it_to == vertices.end()) {
      buffer.Close();
      return false;
    }
    auto edge_accessor = db_accessor.insert_edge(
        it_from->second, it_to->second, db_accessor.edge_type(edge.type));

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(db_accessor.property(property_pair.first),
                             property_pair.second);
  }

  uint64_t hash = buffer.hash();
  if (!buffer.Close()) return false;
  return hash == summary.hash_;
}
