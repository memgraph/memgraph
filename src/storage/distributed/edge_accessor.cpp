#include "storage/edge_accessor.hpp"

#include "database/distributed/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/algorithm.hpp"

EdgeAccessor::EdgeAccessor(EdgeAddress address,
                           database::GraphDbAccessor &db_accessor)
    : RecordAccessor(address, db_accessor, db_accessor.GetEdgeImpl()),
      from_(nullptr),
      to_(nullptr),
      edge_type_() {
  RecordAccessor::Reconstruct();
  if (current_ != nullptr) {
    from_ = current_->from_;
    to_ = current_->to_;
    edge_type_ = current_->edge_type_;
  }
}

EdgeAccessor::EdgeAccessor(EdgeAddress address,
                           database::GraphDbAccessor &db_accessor,
                           VertexAddress from, VertexAddress to,
                           storage::EdgeType edge_type)
    : RecordAccessor(address, db_accessor, db_accessor.GetEdgeImpl()),
      from_(from),
      to_(to),
      edge_type_(edge_type) {}

storage::EdgeType EdgeAccessor::EdgeType() const { return edge_type_; }

VertexAccessor EdgeAccessor::from() const {
  return VertexAccessor(from_, db_accessor());
}

bool EdgeAccessor::from_is(const VertexAccessor &v) const {
  return v.address() == from_;
}

VertexAccessor EdgeAccessor::to() const {
  return VertexAccessor(to_, db_accessor());
}

bool EdgeAccessor::to_is(const VertexAccessor &v) const {
  return v.address() == to_;
}

bool EdgeAccessor::is_cycle() const { return to_ == from_; }

std::ostream &operator<<(std::ostream &os, const EdgeAccessor &ea) {
  os << "E[" << ea.db_accessor().EdgeTypeName(ea.EdgeType());
  os << " {";
  utils::PrintIterable(os, ea.Properties(), ", ", [&](auto &stream,
                                                      const auto &pair) {
    stream << ea.db_accessor().PropertyName(pair.first) << ": " << pair.second;
  });
  return os << "}]";
}
