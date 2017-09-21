#include "storage/edge_accessor.hpp"

#include "database/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/algorithm.hpp"

GraphDbTypes::EdgeType EdgeAccessor::EdgeType() const {
  return current().edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(current().from_, db_accessor());
}

bool EdgeAccessor::from_is(const VertexAccessor &v) const {
  return v.operator==(&current().from_);
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(current().to_, db_accessor());
}

bool EdgeAccessor::to_is(const VertexAccessor &v) const {
  return v.operator==(&current().to_);
}

bool EdgeAccessor::is_cycle() const {
  return &current().to_ == &current().from_;
}

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &ea) {
  os << "E[" << ea.db_accessor().EdgeTypeName(ea.EdgeType());
  os << " {";
  PrintIterable(os, ea.Properties(), ", ", [&](auto &stream, const auto &pair) {
    stream << ea.db_accessor().PropertyName(pair.first) << ": " << pair.second;
  });
  return os << "}]";
}
