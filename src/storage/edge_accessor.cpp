#include "storage/edge_accessor.hpp"

#include "database/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/algorithm.hpp"

GraphDbTypes::EdgeType EdgeAccessor::edge_type() const {
  return current().edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(current().from_, db_accessor());
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(current().to_, db_accessor());
}
bool EdgeAccessor::is_cycle() const {
  return &current().to_ == &current().from_;
}

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &ea) {
  os << "E[" << ea.db_accessor().edge_type_name(ea.edge_type());
  os << " {";
  PrintIterable(os, ea.Properties(), ", ", [&](auto &stream, const auto &pair) {
    stream << ea.db_accessor().property_name(pair.first) << ": " << pair.second;
  });
  return os << "}]";
}
