#include "storage/distributed/edges_iterator.hpp"

#include "storage/distributed/vertex_accessor.hpp"

void EdgeAccessorIterator::CreateOut(const Edges::Element &e) {
  edge_accessor_.emplace(e.edge, va_->db_accessor(), va_->address(), e.vertex,
                         e.edge_type);
}

void EdgeAccessorIterator::CreateIn(const Edges::Element &e) {
  edge_accessor_.emplace(e.edge, va_->db_accessor(), e.vertex, va_->address(),
                         e.edge_type);
}

EdgesIterable::EdgesIterable(
    const VertexAccessor &va, bool from, const VertexAccessor &dest,
    const std::vector<storage::EdgeType> *edge_types) {
  auto sptr = std::make_shared<VertexAccessor>(va);
  begin_.emplace(GetBegin(sptr, from, dest.address(), edge_types));
  end_.emplace(GetEnd(sptr, from));
}

EdgesIterable::EdgesIterable(
    const VertexAccessor &va, bool from,
    const std::vector<storage::EdgeType> *edge_types) {
  auto sptr = std::make_shared<VertexAccessor>(va);
  begin_.emplace(GetBegin(sptr, from, std::experimental::nullopt, edge_types));
  end_.emplace(GetEnd(sptr, from));
}

EdgeAccessorIterator EdgesIterable::GetBegin(
    std::shared_ptr<VertexAccessor> va, bool from,
    std::experimental::optional<storage::VertexAddress> dest,
    const std::vector<storage::EdgeType> *edge_types) {
  const Edges *edges;

  if (from) {
    edges = &va->current().out_;
  } else {
    edges = &va->current().in_;
  }

  return EdgeAccessorIterator(edges->begin(dest, edge_types), va, from);
};

EdgeAccessorIterator EdgesIterable::GetEnd(std::shared_ptr<VertexAccessor> va,
                                           bool from) {
  if (from) {
    auto iter = va->current().out_.end();
    return EdgeAccessorIterator(iter, va, from);
  } else {
    auto iter = va->current().in_.end();
    return EdgeAccessorIterator(iter, va, from);
  }
};
