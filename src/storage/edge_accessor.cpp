#include "storage/edge_accessor.hpp"

#include "database/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/algorithm.hpp"

GraphDbTypes::EdgeType EdgeAccessor::EdgeType() const {
  return current().edge_type_;
}

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(*current().from_.local(), db_accessor());
}

bool EdgeAccessor::from_is(const VertexAccessor &v) const {
  return v == current().from_.local();
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(*current().to_.local(), db_accessor());
}

bool EdgeAccessor::to_is(const VertexAccessor &v) const {
  return v == current().to_.local();
}

bool EdgeAccessor::is_cycle() const { return current().to_ == current().from_; }

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &ea) {
  os << "E[" << ea.db_accessor().EdgeTypeName(ea.EdgeType());
  os << " {";
  utils::PrintIterable(os, ea.Properties(), ", ", [&](auto &stream,
                                                      const auto &pair) {
    stream << ea.db_accessor().PropertyName(pair.first) << ": " << pair.second;
  });
  return os << "}]";
}
