#include "storage/edge_accessor.hpp"

#include "database/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/algorithm.hpp"

GraphDbTypes::EdgeType EdgeAccessor::EdgeType() const { return edge_type_; }

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(*from_.local(), db_accessor());
}

bool EdgeAccessor::from_is(const VertexAccessor &v) const {
  return v == from_.local();
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(*to_.local(), db_accessor());
}

bool EdgeAccessor::to_is(const VertexAccessor &v) const {
  return v == to_.local();
}

bool EdgeAccessor::is_cycle() const { return to_ == from_; }

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &ea) {
  os << "E[" << ea.db_accessor().EdgeTypeName(ea.EdgeType());
  os << " {";
  utils::PrintIterable(os, ea.Properties(), ", ",
                       [&](auto &stream, const auto &pair) {
                         stream << ea.db_accessor().PropertyName(pair.first)
                                << ": " << pair.second;
                       });
  return os << "}]";
}

const Edge &EdgeAccessor::current() {
  if (current_ == nullptr) RecordAccessor::Reconstruct();
  return *current_;
}

const PropertyValueStore<GraphDbTypes::Property> &EdgeAccessor::Properties()
    const {
  if (current_ == nullptr) RecordAccessor::Reconstruct();
  return RecordAccessor::Properties();
}

const PropertyValue &EdgeAccessor::PropsAt(GraphDbTypes::Property key) const {
  if (current_ == nullptr) RecordAccessor::Reconstruct();
  return RecordAccessor::PropsAt(key);
}
