#include "snapshot/snapshot_encoder.hpp"

void SnapshotEncoder::property_name_init(std::string const &name) {
  if (property_name_map.find(name) == property_name_map.end()) {
    auto id = property_name_map.size();
    property_name_map.insert(std::make_pair(name, id));
  }
}

void SnapshotEncoder::label_name_init(std::string const &name) {
  if (label_name_map.find(name) == label_name_map.end()) {
    auto id = label_name_map.size();
    label_name_map.insert(std::make_pair(name, id));
  }
}

void SnapshotEncoder::edge_type_name_init(std::string const &name) {
  if (edge_type_name_map.find(name) == edge_type_name_map.end()) {
    auto id = edge_type_name_map.size();
    edge_type_name_map.insert(std::make_pair(name, id));
  }
}

void SnapshotEncoder::end() { encoder.write_string("end"); }

// **************** INDEX
// Prepares for indexes
void SnapshotEncoder::start_indexes() { encoder.write_string("indexes"); }

// Writes index definition
void SnapshotEncoder::index(IndexDefinition const &def) {
  def.serialize(encoder);
}

// ************* VERTEX
// Prepares for vertices
void SnapshotEncoder::start_vertices() {
  encoder.write_map_header(property_name_map.size());
  for (auto p : property_name_map) {
    encoder.write_string(p.first);
    encoder.write_integer(p.second);
  }

  encoder.write_map_header(label_name_map.size());
  for (auto p : label_name_map) {
    encoder.write_string(p.first);
    encoder.write_integer(p.second);
  }

  encoder.write_map_header(edge_type_name_map.size());
  for (auto p : edge_type_name_map) {
    encoder.write_string(p.first);
    encoder.write_integer(p.second);
  }

  encoder.write_string("vertices");
}

// Starts writing vertex with given id.
void SnapshotEncoder::start_vertex(Id id) { encoder.write_integer(id); }

// Number of following label calls.
void SnapshotEncoder::label_count(size_t n) { encoder.write_list_header(n); }

// Label of currently started vertex.
void SnapshotEncoder::label(std::string const &l) {
  encoder.write_integer(label_name_map.at(l));
}

// ************* EDGE
// Prepares for edges
void SnapshotEncoder::start_edges() { encoder.write_string("edges"); }

// Starts writing edge from vertex to vertex
void SnapshotEncoder::start_edge(Id from, Id to) {
  encoder.write_integer(from);
  encoder.write_integer(to);
}

// Type of currently started edge
void SnapshotEncoder::edge_type(std::string const &et) {
  encoder.write_integer(edge_type_name_map.at(et));
}

// ******* PROPERTY
void SnapshotEncoder::property_count(size_t n) { encoder.write_map_header(n); }

void SnapshotEncoder::property_name(std::string const &name) {
  encoder.write_integer(property_name_map.at(name));
}

void SnapshotEncoder::handle(const Void &v) { encoder.write_null(); }

void SnapshotEncoder::handle(const bool &prop) { encoder.write_bool(prop); }

void SnapshotEncoder::handle(const float &prop) { encoder.write_double(prop); }

void SnapshotEncoder::handle(const double &prop) { encoder.write_double(prop); }

void SnapshotEncoder::handle(const int32_t &prop) {
  encoder.write_integer(prop);
}

void SnapshotEncoder::handle(const int64_t &prop) {
  encoder.write_integer(prop);
}

void SnapshotEncoder::handle(const std::string &value) {
  encoder.write_string(value);
}

void SnapshotEncoder::handle(const ArrayStore<bool> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_bool(e);
  }
}

void SnapshotEncoder::handle(const ArrayStore<int32_t> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_integer(e);
  }
}

void SnapshotEncoder::handle(const ArrayStore<int64_t> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_integer(e);
  }
}

void SnapshotEncoder::handle(const ArrayStore<float> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_double(e);
  }
}

void SnapshotEncoder::handle(const ArrayStore<double> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_double(e);
  }
}

void SnapshotEncoder::handle(const ArrayStore<std::string> &a) {
  encoder.write_list_header(a.size());
  for (auto const &e : a) {
    encoder.write_string(e);
  }
}
