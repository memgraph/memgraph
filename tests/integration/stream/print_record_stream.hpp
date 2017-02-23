#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void write_properties(std::ostream &os, const GraphDbAccessor &access,
                      const TypedValueStore<GraphDb::Property> &properties) {
  if (properties.size() > 0) {
    os << "{";
    for (auto x : properties) {
      os << access.property_name(x.first) << ": " << x.second << ",";
    }
    os << "}\n";
  }
}

std::ostream &operator<<(std::ostream &os, const VertexAccessor &vertex) {
  if (vertex.labels().size() > 0) {
    for (GraphDb::Label label : vertex.labels()) {
      os << vertex.db_accessor().property_name(label) << ", ";
    }
    os << "\n";
  }
  write_properties(os, vertex.db_accessor(), vertex.Properties());
  return os;
}

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &edge) {
  os << "Edge: " << edge.db_accessor().edge_type_name(edge.edge_type()) << "\n";
  write_properties(os, edge.db_accessor(), edge.Properties());
  return os;
}

class PrintRecordStream {
 private:
  std::ostream &stream;

 public:
  PrintRecordStream(std::ostream &stream) : stream(stream) {}

  void write_success() { stream << "SUCCESS\n"; }

  void write_success_empty() { stream << "SUCCESS EMPTY\n"; }

  void write_ignored() { stream << "IGNORED\n"; }

  void write_empty_fields() { stream << "EMPTY FIELDS\n"; }

  void write_fields(const std::vector<std::string> &fields) {
    stream << "FIELDS:";
    for (auto &field : fields) {
      stream << " " << field;
    }
    stream << '\n';
  }

  void write_field(const std::string &field) {
    stream << "Field: " << field << '\n';
  }

  void write(const TypedValue &value) { stream << value; }
  void write_list_header(size_t size) { stream << "List: " << size << '\n'; }

  void write_record() { stream << "Record\n"; }

  void write_vertex_record(const VertexAccessor &vertex) { stream << vertex; }
  void write_edge_record(const EdgeAccessor &edge) { stream << edge; }

  void write_meta(const std::string &type) {
    stream << "Meta: " << type << std::endl;
  }

  void write_failure(const std::map<std::string, std::string> &data) {}

  void write_count(const size_t count) {}
  void chunk() { stream << "CHUNK\n"; }
};
