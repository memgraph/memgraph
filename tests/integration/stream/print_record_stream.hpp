#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "query/backend/cpp/typed_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

void write_properties(std::ostream &os, const GraphDbAccessor &access,
                      const PropertyValueStore<GraphDb::Property> &properties) {
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

  // TODO: all these functions should pretty print their data
  void Header(const std::vector<std::string> &fields) {
    stream << "Header\n";
  }

  void Result(std::vector<TypedValue> &values) {
    stream << "Result\n";
  }

  void Summary(const std::map<std::string, TypedValue> &summary) {
    stream << "Summary\n";
  }
};
